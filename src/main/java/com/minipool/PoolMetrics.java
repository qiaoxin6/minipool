package com.minipool;

import java.util.concurrent.atomic.LongAdder;

/*指标清单：
  ├── borrowCount         总借出次数
  ├── returnCount         总归还次数
  ├── avgBorrowTime       平均借出等待时间（ms）
  ├── maxBorrowTime       最大借出等待时间（ms）
  ├── leakCount           检测到的泄漏次数
  ├── activeCount         当前活跃连接数
  ├── idleCount           当前空闲连接数
  └── totalCount          当前总连接数 */

public class PoolMetrics {
    private final LongAdder borrowCount = new LongAdder();
    private final LongAdder returnCount = new LongAdder();
    private volatile long avgBorrowTime = 0L;
    private volatile long maxBorrowTime = 0L;
    private final LongAdder leakCount = new LongAdder();
    private final LongAdder activeCount = new LongAdder();
    private final LongAdder idleCount = new LongAdder();
    private final LongAdder totalCount = new LongAdder();
    private final LongAdder totalBorrowTime = new LongAdder();

    public LongAdder getBorrowCount() { return borrowCount; }
    public LongAdder getReturnCount() { return returnCount; }
    public long getAvgBorrowTime() { return avgBorrowTime; }
    public void setAvgBorrowTime(long avgBorrowTime) { this.avgBorrowTime = avgBorrowTime; }
    public long getMaxBorrowTime() { return maxBorrowTime; }
    public void setMaxBorrowTime(long maxBorrowTime) { this.maxBorrowTime = maxBorrowTime; }
    public LongAdder getLeakCount() { return leakCount; }
    public LongAdder getActiveCount() { return activeCount; }
    public LongAdder getIdleCount() { return idleCount; }
    public LongAdder getTotalCount() { return totalCount; }
    public LongAdder getTotalBorrowTime() { return totalBorrowTime; }

    @Override
    public String toString() {
        return "PoolMetrics{" +
            "borrowCount=" + borrowCount.sum() +
            ", returnCount=" + returnCount.sum() +
            ", avgBorrowTime=" + avgBorrowTime + "ms" +
            ", maxBorrowTime=" + maxBorrowTime + "ms" +
            ", leakCount=" + leakCount.sum() +
            ", activeCount=" + activeCount.sum() +
            ", idleCount=" + idleCount.sum() +
            ", totalCount=" + totalCount.sum() +
            ", totalBorrowTime=" + totalBorrowTime.sum() + "ms" +
            '}';
    }
}
