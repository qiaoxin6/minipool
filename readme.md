# Mini-Pool 连接池 — 面试复习手册

---

## 一、整体架构

```
┌─────────────────────────────────────────────────────────┐
│                    ConnectionPool                        │
│                                                         │
│  idleQueue (ArrayDeque)    activeSet (HashSet)           │
│  ┌───────┐ ┌───────┐    ┌──────────┐ ┌──────────┐      │
│  │ conn1 │ │ conn2 │    │ proxy A  │ │ proxy B  │      │
│  └───────┘ └───────┘    └──────────┘ └──────────┘      │
│                                                         │
│  totalCount (AtomicInteger) — 全局原子计数器             │
│  returnTimeMap (ConcurrentHashMap) — 空闲连接归还时间    │
│  scheduler (ScheduledExecutorService) — 后台定时任务      │
│  lock (ReentrantLock) + notEmpty (Condition)             │
└─────────────────────────────────────────────────────────┘
          ↑                   ↑
     borrowConnection()   releaseConnection()
          ↓                   ↓
   ┌─────────────┐    ┌─────────────────┐
   │ 池空? 等待   │    │ close() → 归还  │
   │ 池满? 扩容   │    │ 判活? 入池/丢弃  │
   │ 超时? 抛异常 │    │ signal唤醒等待   │
   └─────────────┘    └─────────────────┘
```

### 四大核心类

| 类 | 职责 |
|---|---|
| **ConnectionPool** | 连接池主类：借出、归还、扩容、泄漏检测、空闲回收、指标采集 |
| **PooledConnection** | 动态代理 Handler：拦截 close() → 改为归还，其余方法委托给真实连接 |
| **PoolConfig** | 配置项：池大小、超时时间、泄漏阈值、空闲回收参数 |
| **PoolMetrics** | 运行指标：借出/归还次数、平均/最大等待时间、泄漏次数、实时连接数 |

### 连接生命周期

```
创建(DriverManager) 
  → 放入 idleQueue 
  → borrowConnection() 包成 proxy 放入 activeSet 
  → 业务使用(proxy) 
  → 业务调用 proxy.close() 
  → PooledConnection.invoke 拦截 → releaseConnection() 
  → 判活 → 放回 idleQueue 或丢弃(decrement totalCount)
  → 或 被泄漏检测强制回收
  → 或 被空闲回收清理
  → 或 shutdown() 统一关闭
```

---

## 二、6 个核心设计决策（面试必讲）

### ① 动态代理 vs 继承

```
方案对比：
┌──────────────┬─────────────────────────────┬─────────────────────────────┐
│              │  动态代理 (JDK Proxy)        │  继承 (extends Connection)   │
├──────────────┼─────────────────────────────┼─────────────────────────────┤
│ 耦合度       │ 低，不侵入真实连接           │ 高，强依赖父类实现           │
│ 兼容性       │ 只能代理接口                 │ 需要具体实现类               │
│ 拦截点       │ InvocationHandler.invoke()  │ 需重写每个需要拦截的方法     │
│ 扩展性       │ 可包装任意 Connection 实现   │ 绑定特定驱动实现             │
│ 生产实践     │ HikariCP、Druid 均用此方案  │ 极少使用                    │
└──────────────┴─────────────────────────────┴─────────────────────────────┘
```

**核心逻辑**：只拦截 `close()`，其余全部 `method.invoke(realConnection, args)` 委托。

```java
public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    if ("close".equals(method.getName())) {
        pool.releaseConnection((Connection) proxy);  // 拦截：归还而非关闭
        return null;
    }
    return method.invoke(realConnection, args);       // 委托：透传给真实连接
}
```

**unwrap 实现**：`Proxy.getInvocationHandler(proxy)` → 强转 PooledConnection → 取 realConnection。

> 面试追问：为什么不用 CGLIB？
> - Connection 是接口，JDK Proxy 天然支持
> - CGLIB 通过生成子类拦截，对 final 方法无效
> - JDK Proxy 零额外依赖，性能足够

---

### ② Condition.await vs 自旋等待

```
方案对比：
┌──────────────┬────────────────────────────┬────────────────────────────┐
│              │  Condition.awaitNanos()    │  自旋 (while + Thread.yield)│
├──────────────┼────────────────────────────┼────────────────────────────┤
│ CPU 占用     │ 不等待时零消耗              │ 持续消耗 CPU 周期           │
│ 唤醒延迟     │ 上下文切换成本 ~μs级        │ 零延迟                      │
│ 适用场景     │ 等待时间不确定，可能很长     │ 等待时间极短（锁自旋）       │
│ 唤醒机制     │ signal/signalAll 精确唤醒   │ 轮询检测条件                │
└──────────────┴────────────────────────────┴────────────────────────────┘
```

**为什么必须用 while 循环**（不是 if）：

```java
while (idleQueue.isEmpty() && System.nanoTime() < deadline) {
    notEmpty.awaitNanos(remaining);  // 虚假唤醒(spurious wakeup)后重新检查条件
}
```

虚假唤醒：OS 层面 pthread_cond_wait 可能无 signal 也会返回，JVM 规范明确允许。
用 while 保证唤醒后重新检查条件，不满足则继续等待。

---

### ③ totalCount 的作用 — 扩容的原子占位

```
问题：两个线程同时发现 idleQueue 空，都去扩容 → 超出 maxPoolSize
解决：totalCount.incrementAndGet() 先占位，CAS 语义保证原子性
```

```java
if (totalCount.get() < config.getMaxPoolSize()) {
    totalCount.incrementAndGet();  // ① 先占位（原子操作）
    lock.unlock();                 // ② 释放锁，创建连接是网络IO不持锁
    try {
        Connection con = createRealConnection();  // ③ 锁外创建连接
        lock.lock();
        idleQueue.offer(con);
        notEmpty.signal();
    } catch (SQLException e) {
        totalCount.decrementAndGet();  // ④ 失败必须释放占位
        lock.lock();                   // ⑤ 重新获取锁，否则 finally IllegalMonitorState
    }
}
```

**为什么不用 `activeSet.size() + idleQueue.size()` 代替 totalCount**：
- 这两个集合在锁内，totalCount 在锁外也可用（如 shutdown 时判断）
- 避免每次都加锁才能获取总连接数

---

### ④ isConnectionAlive 放在锁外

```java
// releaseConnection 的关键流程：
lock.lock();
try { activeSet.remove(proxy); }          // 锁内：移出活跃集合
finally { lock.unlock(); }

// ⬇️ 锁外执行：isValid 会发网络包到数据库
if (isConnectionAlive(real)) {
    lock.lock();
    try { idleQueue.offer(real); }        // 锁内：放入空闲队列
    finally { lock.unlock(); }
} else {
    closeQuietly(real);
    totalCount.decrementAndGet();
    // signal...
}
```

**为什么锁外**：`conn.isValid(1)` 会向数据库发送 ping 包（网络 IO，可能耗时数十 ms），
如果持锁执行，所有 borrow/release 线程都被阻塞。

**风险**：归还和判活之间有短暂窗口，连接可能被其他线程借用 → 但这个窗口极短且不影响正确性，
因为归还后该连接还未入 idleQueue，不可能被借用。

---

### ⑤ 泄漏检测 — 快照遍历 + 二次检查（TOCTOU 防护）

```
问题：遍历 activeSet 时，其他线程可能同时归还连接（remove from activeSet）
     直接加锁遍历 → 阻塞所有 borrow/release，不可接受

方案：快照 + 无锁二次检查
```

```java
// 第一步：短暂加锁，复制快照
lock.lock();
try { snapshot = new ArrayList<>(activeSet); }
finally { lock.unlock(); }

// 第二步：无锁遍历快照
for (Connection proxy : snapshot) {
    lock.lock();
    try { isStillActive = activeSet.contains(proxy); }  // 二次检查
    finally { lock.unlock(); }
    
    if (!isStillActive) continue;  // 已归还，跳过
    
    long elapsed = now - handler.getBorrowTime();
    if (elapsed > leakDetectionThreshold)  → 打印告警 + 堆栈
    if (elapsed > leakForceCloseThreshold) → 强制回收
}
```

**forceCloseConnection 为什么用 remove 而不是 contains**：

```java
// ❌ 错误：TOCTOU 竞态
if (activeSet.contains(proxy)) {       // 检查通过
    activeSet.remove(proxy);           // 但执行时可能已被其他线程移除了
    real.close();                      // 关闭了一个已归还的连接 → 数据错乱
}

// ✅ 正确：原子检查+移除
if (!activeSet.remove(proxy)) {        // remove 返回 false 说明已被归还
    return;                            // 安全退出
}
```

---

### ⑥ LongAdder vs AtomicLong

```
场景分析：
- borrowCount / returnCount / leakCount：只 increment，多写少读 → LongAdder
- avgBorrowTime / maxBorrowTime：读取频繁，每次 borrow 都要读 → volatile long
- activeCount / idleCount / totalCount：每次 borrow/release 都 reset+add → LongAdder
```

| 维度 | AtomicLong | LongAdder |
|------|-----------|-----------|
| 原理 | CAS 单一变量 | 分散到多个 Cell，最后求和 |
| 高并发写性能 | 竞争激烈时大量 CAS 失败重试 | 几乎无竞争（不同 Cell） |
| 读取开销 | O(1) 直接读 | O(Cell数) 求和 |
| 适用场景 | 读多写少 | 写多读少 |
| 生产实践 | HikariCP 的 connectionBag 用 AtomicLong | Netty、Disruptor 大量使用 LongAdder |

---

## 三、借出连接完整流程

```
borrowConnection()
│
├─ lock.lock()
│   │
│   ├─ 计算 deadline = now + borrowTimeout
│   │
│   └─ while (idleQueue空 && 未超时)
│       │
│       ├─ totalCount < maxPoolSize ?
│       │   ├─ YES → totalCount++ (占位)
│       │   │        unlock()
│       │   │        创建连接 (网络IO)
│       │   │        lock()
│       │   │        idleQueue.offer(conn)
│       │   │        signal()
│       │   │        break
│       │   │
│       │   └─ NO  → 计算 remaining
│       │            remaining <= 0 → 抛超时异常
│       │            awaitNanos(remaining) ← 释放锁等待
│       │            (被唤醒后回到 while 重新检查)
│       │
│   ├─ idleQueue.poll() → con
│   ├─ returnTimeMap.remove(con)
│   ├─ PooledConnection.wrap(con, pool) → proxy
│   ├─ activeSet.add(proxy)
│   ├─ 更新指标
│   └─ return proxy
│
└─ finally: lock.unlock()
```

**关键点**：
1. while 循环 + awaitNanos = 超时等待 + 防虚假唤醒
2. 扩容时先 unlock 再创建连接 = 网络 IO 不持锁
3. 扩容失败必须 lock.lock() = 恢复锁状态，否则 finally unlock 崩溃
4. totalCount 先加后创建 = 原子占位防超限

---

## 四、归还连接完整流程

```
releaseConnection(proxy)
│
├─ lock.lock()
│   └─ activeSet.remove(proxy)    // 从活跃集合移出
└─ lock.unlock()
│
├─ poolMetrics.returnCount++
├─ PooledConnection.unwrap(proxy) → real
│
├─ isConnectionAlive(real)?       // ⬅️ 锁外执行（网络IO）
│   │
│   ├─ YES → returnTimeMap.put(real, now)  // 记录归还时间（空闲回收用）
│   │        lock.lock()
│   │        idleQueue.offer(real)
│   │        notEmpty.signal()    // 唤醒一个等待线程
│   │        lock.unlock()
│   │
│   └─ NO  → closeQuietly(real)
│           totalCount.decrementAndGet()
│           lock.lock()
│           notEmpty.signal()     // 即使失效也要唤醒（可能触发扩容）
│           lock.unlock()
```

**关键点**：
1. 从 activeSet 移除和放入 idleQueue 是两次独立的加锁 = 缩小锁粒度
2. 连接失效也要 signal = 唤醒等待线程，它们会尝试扩容
3. returnTimeMap 记录归还时间 = 空闲回收线程据此判断是否超时

---

## 五、后台定时任务

### 泄漏检测 (startLeakDetection)

```
触发周期：leakDetectionInterval（默认 10 秒）
检查对象：activeSet 中的每个 proxy

判断逻辑：
├─ 借出时间 < leakDetectionThreshold(60s)  → 正常
├─ 借出时间 > leakDetectionThreshold        → ⚠️ 打印告警 + 借出时堆栈
└─ 借出时间 > leakForceCloseThreshold(120s) → 🔴 强制回收 + leakCount++

实现要点：
- 快照遍历（不长期持锁）
- 二次检查 activeSet.contains（防 TOCTOU）
- forceCloseConnection 用 remove() 而非 contains()（原子检查+移除）
- 堆栈仅在 threshold > 0 时采集（HikariCP 同款策略）
```

### 空闲回收 (startIdleEviction)

```
触发周期：idleEvictionInterval（默认 30 秒）
检查对象：idleQueue 中的连接

判断逻辑：
for each conn in idleQueue:
    ├─ totalCount <= minIdle → break（保底，不再回收）
    ├─ now - returnTime > maxIdleTime(5min) → 回收
    │   ├─ Iterator.remove(conn)
    │   ├─ returnTimeMap.remove(conn)
    │   ├─ closeQuietly(conn)
    │   └─ totalCount.decrementAndGet()
    └─ 否则 continue

实现要点：
- 用 Iterator.remove() 而非 for-each（防 ConcurrentModificationException）
- minIdle 检查用 break 而非 return（Queue 是 FIFO，越早归还越久 → 先回收更合理）
- returnTimeMap 用 ConcurrentHashMap = 空闲回收线程和 release 线程并发访问
- idleQueue 存的是真实 Connection（非 proxy），因为 proxy 每次借出重新创建
```

---

## 六、锁策略总结

```
操作                         锁范围                     原因
──────────────────────────────────────────────────────────────────
borrowConnection 从 idleQueue   全程持锁                  保护 idleQueue + activeSet
borrowConnection 扩容创建连接   先 unlock → 创建 → lock  网络 IO 不持锁
releaseConnection 移出 activeSet  短暂加锁                 缩小锁粒度
releaseConnection isConnectionAlive  锁外执行              网络 IO 不持锁
releaseConnection 放入 idleQueue    短暂加锁               缩小锁粒度
泄漏检测 复制快照                 短暂加锁                 不阻塞业务线程
泄漏检测 二次检查                 短暂加锁                 逐个检查，不长期持锁
空闲回收 遍历清理                 全程持锁                  需要原子修改多个结构
```

**核心原则**：网络 IO 绝对不持锁（创建连接、isValid 检测），锁内只做内存操作。

---

## 七、指标体系 (PoolMetrics)

```
PoolMetrics
├── 累计指标（LongAdder — 写多读少）
│   ├── borrowCount      总借出次数
│   ├── returnCount      总归还次数
│   ├── leakCount        泄漏次数
│   └── totalBorrowTime  总等待时间
│
├── 实时指标（LongAdder — 每次 borrow/release 时 reset+add）
│   ├── activeCount      当前活跃连接数
│   ├── idleCount        当前空闲连接数
│   └── totalCount       当前总连接数
│
└── 统计指标（volatile long — 读多写少）
    ├── avgBorrowTime    平均等待时间
    └── maxBorrowTime    最大等待时间
```

---

## 八、容易踩的坑（Code Review 经验）

| # | 坑 | 正确做法 |
|---|-----|---------|
| 1 | createRealConnection 在锁内执行 | unlock → 创建 → lock |
| 2 | forceCloseConnection 用 contains 再 remove | 直接 remove()，检查返回值 |
| 3 | shutdown() 不关 scheduler | 加 scheduler.shutdownNow() |
| 4 | isConnectionAlive 永远返回 true | 用 `!conn.isClosed() && conn.isValid(1)` |
| 5 | @Data 放在 ConnectionPool 上 | 手写 getter，@Data 会暴露内部可变集合 |
| 6 | 空闲回收用 return 跳出循环 | 用 break（Queue 有序，先回收最老的） |
| 7 | 2 参数构造器不启动后台线程 | 补上 startLeakDetection + startIdleEviction |
| 8 | getTotalCount 用 activeSet+idleQueue | 用 totalCount.get() |
| 9 | for-each 遍历删除元素 | 用 Iterator.remove() |
| 10 | await 用 if 而非 while | 用 while（防虚假唤醒） |
| 11 | 扩容失败不重新加锁 | catch 里 lock.lock() |
| 12 | 始终采集借出堆栈 | threshold > 0 时才采集（HikariCP 策略） |

---

## 九、与 HikariCP 对比（面试加分项）

| 维度 | Mini-Pool | HikariCP |
|------|-----------|----------|
| 借出策略 | FIFO Queue | ConcurrentBag（ThreadLocal + CopyOnWrite） |
| 连接包装 | JDK 动态代理 | 生成代理类（javassist） |
| 等待机制 | Condition.awaitNanos | LockSupport.parkNanos |
| 空闲回收 | ScheduledExecutor 定时 | 延迟调度 houseKeeper |
| 泄漏检测 | 定时快照遍历 | 借出时记录 leakDetectionThreshold |
| 扩容 | borrow 时按需创建 | addConnectionExecutor 异步补充 |
| 指标 | LongAdder | AtomicLong (HikariCP 4.x) |
| 连接验证 | isValid(1) | Connection.isValid + setNetworkTimeout |

---

## 十、面试高频问答

**Q1：为什么连接池需要代理？直接 close 不行吗？**
> 直接 close 就是真正关闭了 TCP 连接，连接池就无法复用。代理拦截 close()，改为归还到池中。

**Q2：扩容时为什么先 unlock 再创建连接？**
> DriverManager.getConnection() 是网络 IO，可能耗时数十 ms。持锁期间所有线程都被阻塞。先占位（totalCount++）保证不超限，unlock 后创建，失败则释放占位。

**Q3：为什么 while 而不是 if？**
> 虚假唤醒（spurious wakeup）。OS 的条件变量可能在没有 signal 的情况下唤醒线程。while 保证唤醒后重新检查条件。

**Q4：泄漏检测为什么不直接遍历 activeSet？**
> 遍历时间长（可能几十个连接），持锁期间所有 borrow/release 被阻塞。用快照复制 + 无锁二次检查，锁持有时间从 O(n) 降到 O(1)。

**Q5：idleQueue 存的是真实 Connection 还是 proxy？**
> 真实 Connection。proxy 每次 borrow 时重新创建包装，因为 proxy 内含 borrowTime 等状态。归还时 unwrap 出真实连接放回队列。

**Q6：LongAdder 和 AtomicLong 怎么选？**
> 写多读少用 LongAdder（分散竞争），读多写少用 AtomicLong。borrowCount 等只 increment 的指标用 LongAdder。
