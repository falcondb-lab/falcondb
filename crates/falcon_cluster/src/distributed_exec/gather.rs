//! Gather-phase merge logic: two-phase aggregation, datum comparison and arithmetic.

use falcon_common::datum::{Datum, OwnedRow};

use super::{AggMerge, ShardResult};

/// Compare two optional Datum values for sorting.
pub fn compare_datums(a: Option<&Datum>, b: Option<&Datum>) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(da), Some(db)) => cmp_datum(da, db),
    }
}

pub(crate) fn cmp_datum(a: &Datum, b: &Datum) -> std::cmp::Ordering {
    match (a, b) {
        (Datum::Null, Datum::Null) => std::cmp::Ordering::Equal,
        (Datum::Null, _) => std::cmp::Ordering::Less,
        (_, Datum::Null) => std::cmp::Ordering::Greater,
        (Datum::Int32(x), Datum::Int32(y)) => x.cmp(y),
        (Datum::Int64(x), Datum::Int64(y)) => x.cmp(y),
        (Datum::Int32(x), Datum::Int64(y)) => (*x as i64).cmp(y),
        (Datum::Int64(x), Datum::Int32(y)) => x.cmp(&(*y as i64)),
        (Datum::Float64(x), Datum::Float64(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (Datum::Float64(x), Datum::Int64(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(std::cmp::Ordering::Equal),
        (Datum::Int64(x), Datum::Float64(y)) => (*x as f64).partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (Datum::Float64(x), Datum::Int32(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(std::cmp::Ordering::Equal),
        (Datum::Int32(x), Datum::Float64(y)) => (*x as f64).partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (Datum::Text(x), Datum::Text(y)) => x.cmp(y),
        (Datum::Boolean(x), Datum::Boolean(y)) => x.cmp(y),
        (Datum::Timestamp(x), Datum::Timestamp(y)) => x.cmp(y),
        (Datum::Date(x), Datum::Date(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    }
}

/// Merge partial aggregation results from multiple shards.
///
/// Each shard produces rows with: [group_by_cols..., agg_cols...].
/// This function groups by the group-by columns and merges each agg column.
pub fn merge_two_phase_agg(
    shard_results: &[ShardResult],
    group_by_indices: &[usize],
    agg_merges: &[AggMerge],
) -> Vec<OwnedRow> {
    use std::collections::HashMap;

    // Use a deterministic binary-encoded group key to avoid Debug-format collisions.
    let mut groups: HashMap<Vec<u8>, Vec<Datum>> = HashMap::new();

    for sr in shard_results {
        for row in &sr.rows {
            let group_key = encode_group_key(group_by_indices, &row.values);

            use std::collections::hash_map::Entry;
            match groups.entry(group_key) {
                Entry::Vacant(e) => {
                    // First row for this group — just store it.
                    e.insert(row.values.clone());
                }
                Entry::Occupied(mut e) => {
                    // Merge aggregate columns into the existing entry.
                    let entry = e.get_mut();
                    for merge in agg_merges {
                        match merge {
                            AggMerge::Sum(idx) | AggMerge::Count(idx) => {
                                let existing = entry.get(*idx).cloned().unwrap_or(Datum::Null);
                                let incoming = row.values.get(*idx).cloned().unwrap_or(Datum::Null);
                                if let Some(val) = entry.get_mut(*idx) {
                                    *val = datum_add(&existing, &incoming);
                                }
                            }
                            AggMerge::Min(idx) => {
                                let existing = entry.get(*idx).cloned().unwrap_or(Datum::Null);
                                let incoming = row.values.get(*idx).cloned().unwrap_or(Datum::Null);
                                if cmp_datum(&incoming, &existing) == std::cmp::Ordering::Less {
                                    if let Some(val) = entry.get_mut(*idx) {
                                        *val = incoming;
                                    }
                                }
                            }
                            AggMerge::Max(idx) => {
                                let existing = entry.get(*idx).cloned().unwrap_or(Datum::Null);
                                let incoming = row.values.get(*idx).cloned().unwrap_or(Datum::Null);
                                if cmp_datum(&incoming, &existing) == std::cmp::Ordering::Greater {
                                    if let Some(val) = entry.get_mut(*idx) {
                                        *val = incoming;
                                    }
                                }
                            }
                            AggMerge::BoolAnd(idx) => {
                                let existing = entry.get(*idx).cloned().unwrap_or(Datum::Null);
                                let incoming = row.values.get(*idx).cloned().unwrap_or(Datum::Null);
                                let merged = match (&existing, &incoming) {
                                    (Datum::Boolean(a), Datum::Boolean(b)) => Datum::Boolean(*a && *b),
                                    (Datum::Null, other) | (other, Datum::Null) => other.clone(),
                                    _ => existing,
                                };
                                if let Some(val) = entry.get_mut(*idx) {
                                    *val = merged;
                                }
                            }
                            AggMerge::BoolOr(idx) => {
                                let existing = entry.get(*idx).cloned().unwrap_or(Datum::Null);
                                let incoming = row.values.get(*idx).cloned().unwrap_or(Datum::Null);
                                let merged = match (&existing, &incoming) {
                                    (Datum::Boolean(a), Datum::Boolean(b)) => Datum::Boolean(*a || *b),
                                    (Datum::Null, other) | (other, Datum::Null) => other.clone(),
                                    _ => existing,
                                };
                                if let Some(val) = entry.get_mut(*idx) {
                                    *val = merged;
                                }
                            }
                            AggMerge::StringAgg(idx, ref sep) => {
                                let existing = entry.get(*idx).cloned().unwrap_or(Datum::Null);
                                let incoming = row.values.get(*idx).cloned().unwrap_or(Datum::Null);
                                let merged = match (&existing, &incoming) {
                                    (Datum::Text(a), Datum::Text(b)) => {
                                        Datum::Text(format!("{}{}{}", a, sep, b))
                                    }
                                    (Datum::Null, other) | (other, Datum::Null) => other.clone(),
                                    _ => existing,
                                };
                                if let Some(val) = entry.get_mut(*idx) {
                                    *val = merged;
                                }
                            }
                            AggMerge::ArrayAgg(idx) => {
                                let existing = entry.get(*idx).cloned().unwrap_or(Datum::Null);
                                let incoming = row.values.get(*idx).cloned().unwrap_or(Datum::Null);
                                let merged = match (existing, &incoming) {
                                    (Datum::Array(mut a), Datum::Array(b)) => {
                                        a.extend(b.iter().cloned());
                                        Datum::Array(a)
                                    }
                                    (Datum::Null, other) => other.clone(),
                                    (other, Datum::Null) => other,
                                    (other, _) => other,
                                };
                                if let Some(val) = entry.get_mut(*idx) {
                                    *val = merged;
                                }
                            }
                            AggMerge::CountDistinct(idx) | AggMerge::SumDistinct(idx) | AggMerge::AvgDistinct(idx) | AggMerge::StringAggDistinct(idx, _) | AggMerge::ArrayAggDistinct(idx) => {
                                // Merge: concatenate arrays of distinct values from shards.
                                let existing = entry.get(*idx).cloned().unwrap_or(Datum::Null);
                                let incoming = row.values.get(*idx).cloned().unwrap_or(Datum::Null);
                                let merged = match (existing, &incoming) {
                                    (Datum::Array(mut a), Datum::Array(b)) => {
                                        a.extend(b.iter().cloned());
                                        Datum::Array(a)
                                    }
                                    (Datum::Null, other) => other.clone(),
                                    (other, Datum::Null) => other,
                                    (other, _) => other,
                                };
                                if let Some(val) = entry.get_mut(*idx) {
                                    *val = merged;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Post-merge fixup for collect-dedup variants: deduplicate arrays and reduce.
    let distinct_fixups: Vec<(&AggMerge, usize)> = agg_merges.iter().filter_map(|m| {
        match m {
            AggMerge::CountDistinct(idx) => Some((m, *idx)),
            AggMerge::SumDistinct(idx) => Some((m, *idx)),
            AggMerge::AvgDistinct(idx) => Some((m, *idx)),
            AggMerge::StringAggDistinct(idx, _) => Some((m, *idx)),
            AggMerge::ArrayAggDistinct(idx) => Some((m, *idx)),
            _ => None,
        }
    }).collect();

    let mut result: Vec<OwnedRow> = groups.into_values().map(OwnedRow::new).collect();
    if !distinct_fixups.is_empty() {
        for row in &mut result {
            for &(merge, idx) in &distinct_fixups {
                if let Some(Datum::Array(arr)) = row.values.get(idx).cloned() {
                    // Deduplicate using binary-encoded keys
                    let mut seen = std::collections::HashSet::new();
                    let unique_vals: Vec<&Datum> = arr.iter().filter(|d| {
                        seen.insert(encode_group_key(&[0], &[(*d).clone()]))
                    }).collect();

                    row.values[idx] = match merge {
                        AggMerge::CountDistinct(_) => Datum::Int64(unique_vals.len() as i64),
                        AggMerge::SumDistinct(_) => {
                            let mut acc = Datum::Null;
                            for val in unique_vals {
                                acc = datum_add(&acc, val);
                            }
                            if acc.is_null() { Datum::Int64(0) } else { acc }
                        }
                        AggMerge::AvgDistinct(_) => {
                            if unique_vals.is_empty() {
                                Datum::Null
                            } else {
                                let count = unique_vals.len() as f64;
                                let mut sum = Datum::Null;
                                for val in unique_vals {
                                    sum = datum_add(&sum, val);
                                }
                                let sum_f64 = match &sum {
                                    Datum::Int32(v) => *v as f64,
                                    Datum::Int64(v) => *v as f64,
                                    Datum::Float64(v) => *v,
                                    _ => 0.0,
                                };
                                Datum::Float64(sum_f64 / count)
                            }
                        }
                        AggMerge::StringAggDistinct(_, ref sep) => {
                            if unique_vals.is_empty() {
                                Datum::Null
                            } else {
                                let parts: Vec<String> = unique_vals.iter().map(|d| format!("{}", d)).collect();
                                Datum::Text(parts.join(sep))
                            }
                        }
                        AggMerge::ArrayAggDistinct(_) => {
                            if unique_vals.is_empty() {
                                Datum::Null
                            } else {
                                Datum::Array(unique_vals.into_iter().cloned().collect())
                            }
                        }
                        _ => unreachable!(),
                    };
                } else if let Some(Datum::Null) = row.values.get(idx) {
                    row.values[idx] = match merge {
                        AggMerge::SumDistinct(_) | AggMerge::AvgDistinct(_) | AggMerge::StringAggDistinct(_, _) | AggMerge::ArrayAggDistinct(_) => Datum::Null,
                        _ => Datum::Int64(0),
                    };
                }
            }
        }
    }
    result
}

/// Encode group-by columns into a deterministic binary key for HashMap grouping.
/// Each datum is prefixed with a type tag byte, followed by its value bytes.
pub(crate) fn encode_group_key(group_by_indices: &[usize], values: &[Datum]) -> Vec<u8> {
    let mut key = Vec::with_capacity(group_by_indices.len() * 9);
    for &i in group_by_indices {
        let datum = values.get(i).unwrap_or(&Datum::Null);
        match datum {
            Datum::Null => key.push(0),
            Datum::Boolean(b) => { key.push(1); key.push(if *b { 1 } else { 0 }); }
            Datum::Int32(v) => { key.push(2); key.extend_from_slice(&v.to_be_bytes()); }
            Datum::Int64(v) => { key.push(3); key.extend_from_slice(&v.to_be_bytes()); }
            Datum::Float64(v) => { key.push(4); key.extend_from_slice(&v.to_be_bytes()); }
            Datum::Text(s) => { key.push(5); key.extend_from_slice(&(s.len() as u32).to_be_bytes()); key.extend_from_slice(s.as_bytes()); }
            Datum::Timestamp(v) => { key.push(6); key.extend_from_slice(&v.to_be_bytes()); }
            Datum::Date(v) => { key.push(9); key.extend_from_slice(&v.to_be_bytes()); }
            Datum::Array(arr) => { key.push(7); let s = format!("{:?}", arr); key.extend_from_slice(&(s.len() as u32).to_be_bytes()); key.extend_from_slice(s.as_bytes()); }
            Datum::Jsonb(v) => { key.push(8); let s = v.to_string(); key.extend_from_slice(&(s.len() as u32).to_be_bytes()); key.extend_from_slice(s.as_bytes()); }
            Datum::Decimal(m, s) => { key.push(10); key.push(*s); key.extend_from_slice(&m.to_be_bytes()); }
            Datum::Time(us) => { key.push(11); key.extend_from_slice(&us.to_be_bytes()); }
            Datum::Interval(mo, d, us) => { key.push(12); key.extend_from_slice(&mo.to_be_bytes()); key.extend_from_slice(&d.to_be_bytes()); key.extend_from_slice(&us.to_be_bytes()); }
            Datum::Uuid(v) => { key.push(13); key.extend_from_slice(&v.to_be_bytes()); }
            Datum::Bytea(bytes) => { key.push(14); key.extend_from_slice(&(bytes.len() as u32).to_be_bytes()); key.extend_from_slice(bytes); }
        }
    }
    key
}

/// Add two Datum values (for SUM/COUNT merge).
pub(crate) fn datum_add(a: &Datum, b: &Datum) -> Datum {
    match (a, b) {
        (Datum::Int32(x), Datum::Int32(y)) => Datum::Int64(*x as i64 + *y as i64),
        (Datum::Int64(x), Datum::Int64(y)) => Datum::Int64(x + y),
        (Datum::Int32(x), Datum::Int64(y)) => Datum::Int64(*x as i64 + y),
        (Datum::Int64(x), Datum::Int32(y)) => Datum::Int64(x + *y as i64),
        (Datum::Float64(x), Datum::Float64(y)) => Datum::Float64(x + y),
        (Datum::Float64(x), Datum::Int64(y)) => Datum::Float64(x + *y as f64),
        (Datum::Int64(x), Datum::Float64(y)) => Datum::Float64(*x as f64 + y),
        (Datum::Float64(x), Datum::Int32(y)) => Datum::Float64(x + *y as f64),
        (Datum::Int32(x), Datum::Float64(y)) => Datum::Float64(*x as f64 + y),
        (Datum::Null, other) | (other, Datum::Null) => other.clone(),
        _ => a.clone(),
    }
}
