use falcon_common::datum::Datum;
use falcon_common::error::ExecutionError;
use falcon_sql_frontend::types::ScalarFunc;
use crate::fts;

pub fn dispatch(func: &ScalarFunc, args: &[Datum]) -> Result<Datum, ExecutionError> {
    match func {
        ScalarFunc::ToTsvector => {
            let text = datum_to_text(&args[0])?;
            let vec = fts::to_tsvector(&text);
            Ok(Datum::TsVector(vec))
        }
        ScalarFunc::ToTsquery => {
            let text = datum_to_text(&args[0])?;
            let q = fts::parse_tsquery(&text)?;
            Ok(Datum::TsQuery(fts::querytree(&q)))
        }
        ScalarFunc::PlaintoTsquery => {
            let text = datum_to_text(&args[0])?;
            let q = fts::plainto_tsquery(&text)?;
            Ok(Datum::TsQuery(fts::querytree(&q)))
        }
        ScalarFunc::PhrastoTsquery => {
            let text = datum_to_text(&args[0])?;
            let q = fts::phraseto_tsquery(&text)?;
            Ok(Datum::TsQuery(fts::querytree(&q)))
        }
        ScalarFunc::TsRank => {
            let (vec, query) = extract_vec_query(args)?;
            let q = fts::parse_tsquery(&query)?;
            Ok(Datum::Float64(fts::ts_rank(&vec, &q)))
        }
        ScalarFunc::TsRankCd => {
            let (vec, query) = extract_vec_query(args)?;
            let q = fts::parse_tsquery(&query)?;
            Ok(Datum::Float64(fts::ts_rank_cd(&vec, &q)))
        }
        ScalarFunc::TsHeadline => {
            if args.len() < 2 {
                return Err(ExecutionError::TypeError("ts_headline requires at least 2 arguments".into()));
            }
            let text = datum_to_text(&args[0])?;
            let query_str = match &args[1] {
                Datum::TsQuery(q) => q.clone(),
                other => datum_to_text(other)?,
            };
            let q = fts::parse_tsquery(&query_str)?;
            Ok(Datum::Text(fts::ts_headline(&text, &q)))
        }
        ScalarFunc::Numnode => {
            let qs = match &args[0] {
                Datum::TsQuery(q) => q.clone(),
                other => datum_to_text(other)?,
            };
            let q = fts::parse_tsquery(&qs)?;
            Ok(Datum::Int32(fts::numnode(&q)))
        }
        ScalarFunc::Querytree => {
            let qs = match &args[0] {
                Datum::TsQuery(q) => q.clone(),
                other => datum_to_text(other)?,
            };
            let q = fts::parse_tsquery(&qs)?;
            Ok(Datum::Text(fts::querytree(&q)))
        }
        ScalarFunc::Setweight => {
            if args.len() < 2 {
                return Err(ExecutionError::TypeError("setweight requires 2 arguments".into()));
            }
            let vec = extract_tsvector(&args[0])?;
            let weight = match &args[1] {
                Datum::Text(s) => s.chars().next().unwrap_or('A'),
                _ => 'A',
            };
            Ok(Datum::TsVector(fts::setweight(&vec, weight)))
        }
        ScalarFunc::Strip => {
            let vec = extract_tsvector(&args[0])?;
            Ok(Datum::TsVector(fts::strip(&vec)))
        }
        ScalarFunc::TsvectorLength => {
            let vec = extract_tsvector(&args[0])?;
            Ok(Datum::Int32(fts::tsvector_length(&vec)))
        }
        ScalarFunc::ArrayToTsvector => {
            match &args[0] {
                Datum::Array(arr) => Ok(Datum::TsVector(fts::array_to_tsvector(arr)?)),
                _ => Err(ExecutionError::TypeError("array_to_tsvector requires array argument".into())),
            }
        }
        ScalarFunc::TsvectorConcat => {
            if args.len() < 2 {
                return Err(ExecutionError::TypeError("tsvector_concat requires 2 arguments".into()));
            }
            let a = extract_tsvector(&args[0])?;
            let b = extract_tsvector(&args[1])?;
            Ok(Datum::TsVector(fts::tsvector_concat(&a, &b)))
        }
        _ => Err(ExecutionError::TypeError(format!("unknown FTS function: {func:?}"))),
    }
}

fn datum_to_text(d: &Datum) -> Result<String, ExecutionError> {
    match d {
        Datum::Text(s) => Ok(s.clone()),
        Datum::Null => Err(ExecutionError::TypeError("NULL argument to FTS function".into())),
        other => Ok(format!("{other}")),
    }
}

fn extract_tsvector(d: &Datum) -> Result<Vec<(String, Vec<u16>)>, ExecutionError> {
    match d {
        Datum::TsVector(v) => Ok(v.clone()),
        Datum::Text(s) => Ok(fts::to_tsvector(s)),
        _ => Err(ExecutionError::TypeError("expected tsvector".into())),
    }
}

fn extract_vec_query(args: &[Datum]) -> Result<(Vec<(String, Vec<u16>)>, String), ExecutionError> {
    if args.len() < 2 {
        return Err(ExecutionError::TypeError("ts_rank requires tsvector and tsquery".into()));
    }
    let vec = extract_tsvector(&args[0])?;
    let query = match &args[1] {
        Datum::TsQuery(q) => q.clone(),
        Datum::Text(s) => s.clone(),
        _ => return Err(ExecutionError::TypeError("expected tsquery".into())),
    };
    Ok((vec, query))
}
