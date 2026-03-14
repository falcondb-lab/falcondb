// crates/falcon_bench/src/dataset.rs
//
// 标准数据集定义与生成器。
// Sequential / Uniform / Zipfian / Hotspot 四种分布，seed 固定保证可重放。

use falcon_common::datum::{Datum, OwnedRow};

// ─── PK 分布 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum PkDistribution {
    /// id = 0, 1, 2, ... N-1  顺序递增，cache 最友好
    Sequential,
    /// 均匀随机 [0, range)，seed 固定
    Uniform { seed: u64, range: u64 },
    /// Zipfian θ（0 < θ < 1，越大越倾斜；0.99 = 极热点）
    /// 使用 Gray & Larson VLDB'94 拒绝采样算法，O(1)/sample
    Zipfian { theta: f64, seed: u64, range: u64 },
    /// 热点分布：hot_fraction 比例的 key 承接 hot_ratio 比例的请求
    Hotspot { hot_fraction: f64, hot_ratio: f64, seed: u64, range: u64 },
}

// ─── 标准数据集规格 ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DatasetSpec {
    pub name:        &'static str,
    pub rows:        u64,
    pub pk_dist:     PkDistribution,
    pub value_bytes: usize,
}

pub const DATASET_TINY: DatasetSpec = DatasetSpec {
    name: "tiny", rows: 1_000,
    pk_dist: PkDistribution::Sequential,
    value_bytes: 16,
};
pub const DATASET_SMALL: DatasetSpec = DatasetSpec {
    name: "small", rows: 100_000,
    pk_dist: PkDistribution::Uniform { seed: 42, range: 100_000 },
    value_bytes: 32,
};
pub const DATASET_MEDIUM: DatasetSpec = DatasetSpec {
    name: "medium", rows: 1_000_000,
    pk_dist: PkDistribution::Uniform { seed: 42, range: 1_000_000 },
    value_bytes: 32,
};
pub const DATASET_LARGE: DatasetSpec = DatasetSpec {
    name: "large", rows: 10_000_000,
    pk_dist: PkDistribution::Uniform { seed: 42, range: 10_000_000 },
    value_bytes: 32,
};
pub const DATASET_HOTSPOT: DatasetSpec = DatasetSpec {
    name: "hotspot", rows: 1_000_000,
    pk_dist: PkDistribution::Zipfian { theta: 0.99, seed: 42, range: 1_000_000 },
    value_bytes: 32,
};
pub const DATASET_SKEWED: DatasetSpec = DatasetSpec {
    name: "skewed", rows: 1_000_000,
    pk_dist: PkDistribution::Zipfian { theta: 0.70, seed: 42, range: 1_000_000 },
    value_bytes: 32,
};

impl DatasetSpec {
    /// 生成用于预填充的行迭代器
    pub fn row_iter(&self) -> impl Iterator<Item = OwnedRow> + '_ {
        let mut rng = XorShift64::new(
            match &self.pk_dist {
                PkDistribution::Sequential => 0,
                PkDistribution::Uniform { seed, .. }
                | PkDistribution::Zipfian { seed, .. }
                | PkDistribution::Hotspot { seed, .. } => *seed,
            }
        );
        let value_bytes = self.value_bytes;
        (0..self.rows).map(move |i| {
            let pk = i as i64; // sequential for initial load
            let balance = (rng.next() % 1_000_000) as i64;
            let name_suffix = format!("{pk:0>width$}", width = value_bytes.min(20));
            OwnedRow::new(vec![
                Datum::Int64(pk),
                Datum::Int64(balance),
                Datum::Text(format!("user_{name_suffix}")),
            ])
        })
    }

    /// 生成 workload PK 序列（用于 bench 请求分布）
    pub fn pk_sampler(&self) -> PkSampler {
        PkSampler::new(&self.pk_dist)
    }
}

// ─── PK 采样器 ───────────────────────────────────────────────────────────────

pub struct PkSampler {
    inner: SamplerKind,
}

enum SamplerKind {
    Sequential { next: u64, range: u64 },
    Uniform    { rng: XorShift64, range: u64 },
    Zipfian    { z: ZipfianSampler },
    Hotspot    { rng: XorShift64, hot_fraction: f64, hot_ratio: f64, range: u64 },
}

impl PkSampler {
    pub fn new(dist: &PkDistribution) -> Self {
        let inner = match dist {
            PkDistribution::Sequential =>
                SamplerKind::Sequential { next: 0, range: u64::MAX },
            PkDistribution::Uniform { seed, range } =>
                SamplerKind::Uniform { rng: XorShift64::new(*seed), range: *range },
            PkDistribution::Zipfian { theta, seed, range } =>
                SamplerKind::Zipfian { z: ZipfianSampler::new(*theta, *seed, *range) },
            PkDistribution::Hotspot { hot_fraction, hot_ratio, seed, range } =>
                SamplerKind::Hotspot {
                    rng: XorShift64::new(*seed),
                    hot_fraction: *hot_fraction,
                    hot_ratio:    *hot_ratio,
                    range:        *range,
                },
        };
        Self { inner }
    }

    #[inline]
    pub fn next_pk(&mut self) -> i64 {
        match &mut self.inner {
            SamplerKind::Sequential { next, range } => {
                let v = *next % *range;
                *next = next.wrapping_add(1);
                v as i64
            }
            SamplerKind::Uniform { rng, range } =>
                (rng.next() % *range) as i64,
            SamplerKind::Zipfian { z } =>
                z.next() as i64,
            SamplerKind::Hotspot { rng, hot_fraction, hot_ratio, range } => {
                let u = rng.next_f64();
                if u < *hot_ratio {
                    let hot_range = ((*range as f64) * *hot_fraction) as u64;
                    (rng.next() % hot_range.max(1)) as i64
                } else {
                    (rng.next() % *range) as i64
                }
            }
        }
    }
}

// ─── Zipfian 采样器（Gray & Larson VLDB'94）──────────────────────────────────

pub struct ZipfianSampler {
    theta:  f64,
    range:  u64,
    zetan:  f64,
    alpha:  f64,
    eta:    f64,
    rng:    XorShift64,
}

impl ZipfianSampler {
    pub fn new(theta: f64, seed: u64, range: u64) -> Self {
        let zetan   = Self::zeta(range, theta);
        let zeta2   = Self::zeta(2, theta);
        let alpha   = 1.0 / (1.0 - theta);
        let eta     = (1.0 - (2.0_f64 / range as f64).powf(1.0 - theta))
                      / (1.0 - zeta2 / zetan);
        Self { theta, range, zetan, alpha, eta, rng: XorShift64::new(seed) }
    }

    fn zeta(n: u64, theta: f64) -> f64 {
        let mut sum = 0.0f64;
        for i in 1..=(n.min(100_000)) {
            sum += 1.0 / (i as f64).powf(theta);
        }
        if n > 100_000 {
            // 近似尾部：∫_{100001}^{n} x^{-θ} dx
            let lo = 100_001u64;
            let hi = n;
            sum += ((hi as f64).powf(1.0 - theta) - (lo as f64).powf(1.0 - theta))
                   / (1.0 - theta);
        }
        sum
    }

    #[inline]
    pub fn next(&mut self) -> u64 {
        let u  = self.rng.next_f64();
        let uz = u * self.zetan;
        let rank = if uz < 1.0 {
            0u64
        } else if uz < 1.0 + 0.5f64.powf(self.theta) {
            1u64
        } else {
            (self.range as f64 * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as u64
        };
        rank.min(self.range - 1)
    }
}

// ─── xorshift64 PRNG（无外部依赖，确定性）──────────────────────────────────

pub struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    pub fn new(seed: u64) -> Self {
        Self { state: if seed == 0 { 0xcafe_babe_dead_beef } else { seed } }
    }

    #[inline]
    pub fn next(&mut self) -> u64 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        self.state
    }

    #[inline]
    pub fn next_f64(&mut self) -> f64 {
        // 生成 [0, 1) 浮点数（用高 53 位）
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xorshift_deterministic() {
        let mut a = XorShift64::new(42);
        let mut b = XorShift64::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next(), b.next());
        }
    }

    #[test]
    fn uniform_sampler_range() {
        let spec = DATASET_SMALL.clone();
        let mut s = spec.pk_sampler();
        for _ in 0..10_000 {
            let pk = s.next_pk();
            assert!(pk >= 0 && pk < 100_000);
        }
    }

    #[test]
    fn zipfian_sampler_range() {
        let spec = DATASET_HOTSPOT.clone();
        let mut s = spec.pk_sampler();
        for _ in 0..10_000 {
            let pk = s.next_pk();
            assert!(pk >= 0 && pk < 1_000_000);
        }
    }

    #[test]
    fn zipfian_is_skewed() {
        // 统计前 1% key 的命中率，θ=0.99 应接近 ~60%
        let mut z = ZipfianSampler::new(0.99, 1, 1_000_000);
        let mut hot = 0u64;
        let total = 100_000u64;
        let hot_bound = 10_000u64; // 前 1%
        for _ in 0..total {
            if z.next() < hot_bound { hot += 1; }
        }
        let ratio = hot as f64 / total as f64;
        assert!(ratio > 0.50, "Zipfian θ=0.99 应有 >50% 流量打到前1% key，实际: {ratio:.2}");
    }

    #[test]
    fn dataset_row_iter_count() {
        let spec = DATASET_TINY.clone();
        let count = spec.row_iter().count();
        assert_eq!(count as u64, spec.rows);
    }
}
