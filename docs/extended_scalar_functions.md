# Extended Scalar Functions

> Extracted from ARCHITECTURE.md section 9 to reduce file size.
> See [ARCHITECTURE.md](../ARCHITECTURE.md) for the core architecture documentation.

## 9. Extended Scalar Functions

### 9.1 Array Matrix Functions (P369)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_RANGE25(arr, rows, cols, row_idx)` | Range (max − min) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_NORM25(arr, rows, cols, col_idx)` | L1 norm (sum of absolute values) of the specified column |

### 9.2 String Encoding Functions (P369)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_WWYV_ENCODE(fields...)` | `&` | Encode fields into `&`-separated text with `\`-escaping |
| `STRING_WWYV_DECODE(line, idx)` | `&` | Decode field at index from `&`-separated text with unescaping |

### 9.3 Array Matrix Functions (P370)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_DEV25(arr, rows, cols, row_idx)` | Mean absolute deviation of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE25(arr, rows, cols, col_idx)` | Range of ln(\|v\|) values in the specified column |

### 9.4 String Encoding Functions (P370)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_XXYV_ENCODE(fields...)` | `%` | Encode fields into `%`-separated text with `\`-escaping |
| `STRING_XXYV_DECODE(line, idx)` | `%` | Decode field at index from `%`-separated text with unescaping |

### 9.5 Array Matrix Functions (P371)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_NORM25(arr, rows, cols, row_idx)` | Sum of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_DEV25(arr, rows, cols, col_idx)` | Population standard deviation of values in the specified column |

### 9.6 String Encoding Functions (P371)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_YYYV_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_YYYV_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.7 Array Matrix Functions (P372)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_RANGE25(arr, rows, cols, row_idx)` | Range of \|v\| (max\|v\| − min\|v\|) in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_NORM25(arr, rows, cols, col_idx)` | Sum of ln(\|v\|) for non-zero values in the specified column |

### 9.8 String Encoding Functions (P372)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_ZZYV_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_ZZYV_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.9 Array Matrix Functions (P373)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM26(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_DEV26(arr, rows, cols, col_idx)` | Mean absolute deviation of values in the specified column |

### 9.10 String Encoding Functions (P373)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_AAZV_ENCODE(fields...)` | `\|` | Encode fields into `\|`-separated text with `\`-escaping |
| `STRING_AAZV_DECODE(line, idx)` | `\|` | Decode field at index from `\|`-separated text with unescaping |

### 9.11 Array Matrix Functions (P374)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_DEV26(arr, rows, cols, row_idx)` | Mean absolute deviation of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE26(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.12 String Encoding Functions (P374)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_BBZV_ENCODE(fields...)` | `;` | Encode fields into `;`-separated text with `\`-escaping |
| `STRING_BBZV_DECODE(line, idx)` | `;` | Decode field at index from `;`-separated text with unescaping |

### 9.13 Array Matrix Functions (P375)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_RANGE26(arr, rows, cols, row_idx)` | Range of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_NORM26(arr, rows, cols, col_idx)` | RMS (root mean square) of values in the specified column |

### 9.14 String Encoding Functions (P375)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_CCZV_ENCODE(fields...)` | `:` | Encode fields into `:`-separated text with `\`-escaping |
| `STRING_CCZV_DECODE(line, idx)` | `:` | Decode field at index from `:`-separated text with unescaping |

### 9.15 Array Matrix Functions (P376)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_RANGE27(arr, rows, cols, row_idx)` | Range (max − min) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_NORM27(arr, rows, cols, col_idx)` | L1 norm (sum of absolute values) of the specified column |

### 9.16 String Encoding Functions (P376)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_DDZV_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_DDZV_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.17 Array Matrix Functions (P377)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_DEV27(arr, rows, cols, row_idx)` | Mean absolute deviation of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE27(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.18 String Encoding Functions (P377)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_EEZV_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_EEZV_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.19 Array Matrix Functions (P378)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_DEV27(arr, rows, cols, row_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_DEV27(arr, rows, cols, col_idx)` | Mean absolute deviation of values in the specified column |

### 9.20 String Encoding Functions (P378)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_FFZV_ENCODE(fields...)` | `%` | Encode fields into `%`-separated text with `\`-escaping |
| `STRING_FFZV_DECODE(line, idx)` | `%` | Decode field at index from `%`-separated text with unescaping |

### 9.21 Array Matrix Functions (P379)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_RANGE28(arr, rows, cols, row_idx)` | Range (max − min) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_NORM28(arr, rows, cols, col_idx)` | L1 norm (sum of absolute values) of the specified column |

### 9.22 String Encoding Functions (P379)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_GGZV_ENCODE(fields...)` | `@` | Encode fields into `@`-separated text with `\`-escaping |
| `STRING_GGZV_DECODE(line, idx)` | `@` | Decode field at index from `@`-separated text with unescaping |

### 9.23 Array Matrix Functions (P380)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_RANGE28(arr, rows, cols, row_idx)` | Range of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_NORM28(arr, rows, cols, col_idx)` | RMS (root mean square) of values in the specified column |

### 9.24 String Encoding Functions (P380)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_HHZV_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_HHZV_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.25 Array Matrix Functions (P381)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_RANGE28(arr, rows, cols, row_idx)` | Range of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE28(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.26 String Encoding Functions (P381)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_IIZV_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_IIZV_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.27 Array Matrix Functions (P382)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_DEV28(arr, rows, cols, row_idx)` | Standard deviation of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_DEV28(arr, rows, cols, col_idx)` | Mean absolute deviation of values in the specified column |

### 9.28 String Encoding Functions (P382)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_JJZV_ENCODE(fields...)` | `` ` `` | Encode fields into `` ` ``-separated text with `\`-escaping |
| `STRING_JJZV_DECODE(line, idx)` | `` ` `` | Decode field at index from `` ` ``-separated text with unescaping |

### 9.29 Array Matrix Functions (P383)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_DEV28(arr, rows, cols, row_idx)` | Standard deviation of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_DEV28(arr, rows, cols, col_idx)` | Standard deviation of values in the specified column |

### 9.30 String Encoding Functions (P383)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_KKZV_ENCODE(fields...)` | `%` | Encode fields into `%`-separated text with `\`-escaping |
| `STRING_KKZV_DECODE(line, idx)` | `%` | Decode field at index from `%`-separated text with unescaping |

### 9.31 Array Matrix Functions (P384)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM28(arr, rows, cols, row_idx)` | L1 norm (sum of absolute values) of the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_NORM28(arr, rows, cols, col_idx)` | Sum of ln(\|v\|) for non-zero values in the specified column |

### 9.32 String Encoding Functions (P384)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_LLZV_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_LLZV_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.33 Array Matrix Functions (P385)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_RANGE29(arr, rows, cols, row_idx)` | Range (max − min) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_NORM29(arr, rows, cols, col_idx)` | L1 norm (sum of absolute values) of the specified column |

### 9.34 String Encoding Functions (P385)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_MMZV_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_MMZV_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.35 Array Matrix Functions (P386)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_RANGE29(arr, rows, cols, row_idx)` | Range of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_NORM29(arr, rows, cols, col_idx)` | RMS (root mean square) of values in the specified column |

### 9.36 String Encoding Functions (P386)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_NNZV_ENCODE(fields...)` | `@` | Encode fields into `@`-separated text with `\`-escaping |
| `STRING_NNZV_DECODE(line, idx)` | `@` | Decode field at index from `@`-separated text with unescaping |

### 9.37 Array Matrix Functions (P387)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_RANGE29(arr, rows, cols, row_idx)` | Range of absolute values (max\|v\| − min\|v\|) in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE29(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.38 String Encoding Functions (P387)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_OOZV_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_OOZV_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.39 Array Matrix Functions (P388)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_RANGE30(arr, rows, cols, row_idx)` | Range (max − min) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_NORM30(arr, rows, cols, col_idx)` | L1 norm (sum of absolute values) of the specified column |

### 9.40 String Encoding Functions (P388)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_PPZV_ENCODE(fields...)` | `` ` `` | Encode fields into `` ` ``-separated text with `\`-escaping |
| `STRING_PPZV_DECODE(line, idx)` | `` ` `` | Decode field at index from `` ` ``-separated text with unescaping |

### 9.41 Array Matrix Functions (P389)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_NORM30(arr, rows, cols, row_idx)` | Sum of ln(|v|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_DEV30(arr, rows, cols, col_idx)` | Standard deviation of values in the specified column |

### 9.42 String Encoding Functions (P389)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_QQZV_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_QQZV_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.43 Array Matrix Functions (P390)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM30(arr, rows, cols, row_idx)` | L1 norm (sum of absolute values) of the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_NORM30(arr, rows, cols, col_idx)` | Sum of ln(|v|) for non-zero values in the specified column |

### 9.44 String Encoding Functions (P390)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_RRZV_ENCODE(fields...)` | `%` | Encode fields into `%`-separated text with `\`-escaping |
| `STRING_RRZV_DECODE(line, idx)` | `%` | Decode field at index from `%`-separated text with unescaping |

### 9.45 Array Matrix Functions (P391)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_RANGE31(arr, rows, cols, row_idx)` | Range (max − min) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_NORM31(arr, rows, cols, col_idx)` | L1 norm (sum of absolute values) of the specified column |

### 9.46 String Encoding Functions (P391)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_SSZV_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_SSZV_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.47 Array Matrix Functions (P392)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_NORM31(arr, rows, cols, row_idx)` | Sum of ln(|v|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_DEV31(arr, rows, cols, col_idx)` | Standard deviation of values in the specified column |

### 9.48 String Encoding Functions (P392)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_TTZV_ENCODE(fields...)` | `` ` `` | Encode fields into `` ` ``-separated text with `\`-escaping |
| `STRING_TTZV_DECODE(line, idx)` | `` ` `` | Decode field at index from `` ` ``-separated text with unescaping |

### 9.49 Array Matrix Functions (P393)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_DEV31(arr, rows, cols, row_idx)` | Mean absolute deviation of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE31(arr, rows, cols, col_idx)` | Range of ln(|v|) for non-zero values in the specified column |

### 9.50 String Encoding Functions (P393)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_UUZV_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_UUZV_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.51 Array Matrix Functions (P394)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM31(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_DEV31(arr, rows, cols, col_idx)` | Mean absolute deviation of values in the specified column |

### 9.52 String Encoding Functions (P394)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_VVZV_ENCODE(fields...)` | `%` | Encode fields into `%`-separated text with `\`-escaping |
| `STRING_VVZV_DECODE(line, idx)` | `%` | Decode field at index from `%`-separated text with unescaping |

### 9.53 Array Matrix Functions (P395)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_RANGE31(arr, rows, cols, row_idx)` | Range of ln(|v|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_NORM31(arr, rows, cols, col_idx)` | RMS (root mean square) of values in the specified column |

### 9.54 String Encoding Functions (P395)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_WWZV_ENCODE(fields...)` | `=` | Encode fields into `=`-separated text with `\`-escaping |
| `STRING_WWZV_DECODE(line, idx)` | `=` | Decode field at index from `=`-separated text with unescaping |

### 9.55 Array Matrix Functions (P396)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM31(arr, rows, cols, row_idx)` | L1 norm (sum of absolute values) of the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV31(arr, rows, cols, col_idx)` | Standard deviation of ln(|v|) for non-zero values in the specified column |

### 9.56 String Encoding Functions (P396)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_XXZV_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_XXZV_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.57 Array Matrix Functions (P397)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_RANGE32(arr, rows, cols, row_idx)` | Range (max − min) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_NORM32(arr, rows, cols, col_idx)` | L1 norm (sum of absolute values) of the specified column |

### 9.58 String Encoding Functions (P397)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_YYZV_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_YYZV_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.59 Array Matrix Functions (P398)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_RANGE32(arr, rows, cols, row_idx)` | Range of ln(|v|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_NORM32(arr, rows, cols, col_idx)` | RMS (root mean square) of values in the specified column |

### 9.60 String Encoding Functions (P398)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_ZZZV_ENCODE(fields...)` | `` ` `` | Encode fields into `` ` ``-separated text with `\`-escaping |
| `STRING_ZZZV_DECODE(line, idx)` | `` ` `` | Decode field at index from `` ` ``-separated text with unescaping |

### 9.61 Array Matrix Functions (P399)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_DEV32(arr, rows, cols, row_idx)` | Mean absolute deviation of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE32(arr, rows, cols, col_idx)` | Range of ln(|v|) for non-zero values in the specified column |

### 9.62 String Encoding Functions (P399)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_AAAW_ENCODE(fields...)` | `%` | Encode fields into `%`-separated text with `\`-escaping |
| `STRING_AAAW_DECODE(line, idx)` | `%` | Decode field at index from `%`-separated text with unescaping |

### 9.63 Array Matrix Functions (P400)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM32(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_DEV32(arr, rows, cols, col_idx)` | Mean absolute deviation of values in the specified column |

### 9.64 String Encoding Functions (P400)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_BBAW_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_BBAW_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.65 Array Matrix Functions (P401)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_NORM32(arr, rows, cols, row_idx)` | Sum of ln(|v|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_RANGE32(arr, rows, cols, col_idx)` | Range (max − min) of values in the specified column |

### 9.66 String Encoding Functions (P401)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_CCAW_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_CCAW_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.67 Array Matrix Functions (P402)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_RANGE32(arr, rows, cols, row_idx)` | Range (max − min) of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_NORM32(arr, rows, cols, col_idx)` | Sum of ln(|v|) for non-zero values in the specified column |

### 9.68 String Encoding Functions (P402)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_DDAW_ENCODE(fields...)` | `=` | Encode fields into `=`-separated text with `\`-escaping |
| `STRING_DDAW_DECODE(line, idx)` | `=` | Decode field at index from `=`-separated text with unescaping |

### 9.69 Array Matrix Functions (P403)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_DEV32(arr, rows, cols, row_idx)` | Standard deviation of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_RANGE32(arr, rows, cols, col_idx)` | Range (max − min) of absolute values in the specified column |

### 9.70 String Encoding Functions (P403)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_EEAW_ENCODE(fields...)` | `%` | Encode fields into `%`-separated text with `\`-escaping |
| `STRING_EEAW_DECODE(line, idx)` | `%` | Decode field at index from `%`-separated text with unescaping |

### 9.71 Array Matrix Functions (P404)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_RANGE33(arr, rows, cols, row_idx)` | Range of ln(\|v\|) values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_DEV33(arr, rows, cols, col_idx)` | Standard deviation of values in the specified column |

### 9.72 String Encoding Functions (P404)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_FFAW_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_FFAW_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.73 Array Matrix Functions (P405)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_RANGE34(arr, rows, cols, row_idx)` | Range (max − min) of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_NORM34(arr, rows, cols, col_idx)` | Sum of ln(\|v\|) for non-zero values in the specified column |

### 9.74 String Encoding Functions (P405)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_GGAW_ENCODE(fields...)` | `=` | Encode fields into `=`-separated text with `\`-escaping |
| `STRING_GGAW_DECODE(line, idx)` | `=` | Decode field at index from `=`-separated text with unescaping |

### 9.75 Array Matrix Functions (P406)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_NORM35(arr, rows, cols, row_idx)` | Sum of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_DEV35(arr, rows, cols, col_idx)` | Mean absolute deviation of values in the specified column |

### 9.76 String Encoding Functions (P406)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_HHAW_ENCODE(fields...)` | `\|` | Encode fields into `\|`-separated text with `\`-escaping |
| `STRING_HHAW_DECODE(line, idx)` | `\|` | Decode field at index from `\|`-separated text with unescaping |

### 9.77 Array Matrix Functions (P407)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_RANGE35(arr, rows, cols, row_idx)` | Range (max − min) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV35(arr, rows, cols, col_idx)` | Standard deviation of ln(\|v\|) for non-zero values in the specified column |

### 9.78 String Encoding Functions (P407)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_IIAW_ENCODE(fields...)` | `:` | Encode fields into `:`-separated text with `\`-escaping |
| `STRING_IIAW_DECODE(line, idx)` | `:` | Decode field at index from `:`-separated text with unescaping |

### 9.79 Array Matrix Functions (P408)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM35(arr, rows, cols, row_idx)` | L1 norm (sum of absolute values) of the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_RANGE35(arr, rows, cols, col_idx)` | Range (max − min) of values in the specified column |

### 9.80 String Encoding Functions (P408)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_JJAW_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_JJAW_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.81 Array Matrix Functions (P409)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_NORM36(arr, rows, cols, row_idx)` | Sum of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_DEV36(arr, rows, cols, col_idx)` | Mean absolute deviation of values in the specified column |

### 9.82 String Encoding Functions (P409)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_KKAW_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_KKAW_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.83 Array Matrix Functions (P410)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM36(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_NORM36(arr, rows, cols, col_idx)` | Sum of ln(\|v\|) for non-zero values in the specified column |

### 9.84 String Encoding Functions (P410)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_LLAW_ENCODE(fields...)` | `@` | Encode fields into `@`-separated text with `\`-escaping |
| `STRING_LLAW_DECODE(line, idx)` | `@` | Decode field at index from `@`-separated text with unescaping |

### 9.85 Array Matrix Functions (P411)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_DEV36(arr, rows, cols, row_idx)` | Mean absolute deviation of values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_NORM36(arr, rows, cols, col_idx)` | RMS (root mean square) of values in the specified column |

### 9.86 String Encoding Functions (P411)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_MMAW_ENCODE(fields...)` | `%` | Encode fields into `%`-separated text with `\`-escaping |
| `STRING_MMAW_DECODE(line, idx)` | `%` | Decode field at index from `%`-separated text with unescaping |

### 9.87 Array Matrix Functions (P412)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_DEV36(arr, rows, cols, row_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_RANGE36(arr, rows, cols, col_idx)` | Range of absolute values (max\|v\| − min\|v\|) in the specified column |

### 9.88 String Encoding Functions (P412)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_NNAW_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_NNAW_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.89 Array Matrix Functions (P413)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_RANGE36(arr, rows, cols, row_idx)` | Range (max − min) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV36(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.90 String Encoding Functions (P413)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_OOAW_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_OOAW_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.91 Array Matrix Functions (P414)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM37(arr, rows, cols, row_idx)` | L1 norm (sum of absolute values) of the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_DEV37(arr, rows, cols, col_idx)` | Standard deviation of values in the specified column |

### 9.92 String Encoding Functions (P414)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_PPAW_ENCODE(fields...)` | `=` | Encode fields into `=`-separated text with `\`-escaping |
| `STRING_PPAW_DECODE(line, idx)` | `=` | Decode field at index from `=`-separated text with unescaping |

### 9.93 Array Matrix Functions (P415)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_DEV37(arr, rows, cols, row_idx)` | Standard deviation of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_NORM37(arr, rows, cols, col_idx)` | L1 norm (sum of absolute values) of the specified column |

### 9.94 String Encoding Functions (P415)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_QQAW_ENCODE(fields...)` | `:` | Encode fields into `:`-separated text with `\`-escaping |
| `STRING_QQAW_DECODE(line, idx)` | `:` | Decode field at index from `:`-separated text with unescaping |

### 9.95 Array Matrix Functions (P416)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_NORM37(arr, rows, cols, row_idx)` | Sum of ln(|v|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_RANGE37(arr, rows, cols, col_idx)` | Range (max − min) of values in the specified column |

### 9.96 String Encoding Functions (P416)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_RRAW_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_RRAW_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.97 Array Matrix Functions (P417)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_DEV37(arr, rows, cols, row_idx)` | Mean absolute deviation of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_NORM37(arr, rows, cols, col_idx)` | Sum of ln(|v|) for non-zero values in the specified column |

### 9.98 String Encoding Functions (P417)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_SSAW_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_SSAW_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.99 Array Matrix Functions (P418)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_DEV37(arr, rows, cols, row_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_DEV37(arr, rows, cols, col_idx)` | Mean absolute deviation of values in the specified column |

### 9.100 String Encoding Functions (P418)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_TTAW_ENCODE(fields...)` | `=` | Encode fields into `=`-separated text with `\`-escaping |
| `STRING_TTAW_DECODE(line, idx)` | `=` | Decode field at index from `=`-separated text with unescaping |

### 9.101 Array Matrix Functions (P419)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM37(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV37(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.102 String Encoding Functions (P419)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_UUAW_ENCODE(fields...)` | `%` | Encode fields into `%`-separated text with `\`-escaping |
| `STRING_UUAW_DECODE(line, idx)` | `%` | Decode field at index from `%`-separated text with unescaping |

### 9.103 Array Matrix Functions (P420)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_RANGE38(arr, rows, cols, row_idx)` | Range (max − min) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_NORM38(arr, rows, cols, col_idx)` | L1 norm (sum of absolute values) of the specified column |

### 9.104 String Encoding Functions (P420)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_VVAW_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_VVAW_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.105 Array Matrix Functions (P421)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM39(arr, rows, cols, row_idx)` | L1 norm (sum of absolute values) of the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_DEV39(arr, rows, cols, col_idx)` | Population standard deviation of values in the specified column |

### 9.106 String Encoding Functions (P421)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_WWAW_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_WWAW_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.107 Array Matrix Functions (P422)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_DEV39(arr, rows, cols, row_idx)` | Population standard deviation of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_NORM39(arr, rows, cols, col_idx)` | L1 norm (sum of absolute values) of the specified column |

### 9.108 String Encoding Functions (P422)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_XXAW_ENCODE(fields...)` | `@` | Encode fields into `@`-separated text with `\`-escaping |
| `STRING_XXAW_DECODE(line, idx)` | `@` | Decode field at index from `@`-separated text with unescaping |

### 9.109 Array Matrix Functions (P423)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_NORM39(arr, rows, cols, row_idx)` | Sum of ln(|v|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_RANGE39(arr, rows, cols, col_idx)` | Range (max - min) of values in the specified column |

### 9.110 String Encoding Functions (P423)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_YYAW_ENCODE(fields...)` | `=` | Encode fields into `=`-separated text with `\`-escaping |
| `STRING_YYAW_DECODE(line, idx)` | `=` | Decode field at index from `=`-separated text with unescaping |

### 9.111 Array Matrix Functions (P424)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_DEV39(arr, rows, cols, row_idx)` | Mean absolute deviation of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_NORM39(arr, rows, cols, col_idx)` | Sum of ln(\|v\|) for non-zero values in the specified column |

### 9.112 String Encoding Functions (P424)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_ZZAW_ENCODE(fields...)` | `%` | Encode fields into `%`-separated text with `\`-escaping |
| `STRING_ZZAW_DECODE(line, idx)` | `%` | Decode field at index from `%`-separated text with unescaping |

### 9.113 Array Matrix Functions (P425)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_RANGE39(arr, rows, cols, row_idx)` | Range (max − min) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_DEV39(arr, rows, cols, col_idx)` | Mean absolute deviation of values in the specified column |

### 9.114 String Encoding Functions (P425)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_AABW_ENCODE(fields...)` | `\|` | Encode fields into `\|`-separated text with `\`-escaping |
| `STRING_AABW_DECODE(line, idx)` | `\|` | Decode field at index from `\|`-separated text with unescaping |

### 9.115 Array Matrix Functions (P426)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_DEV39(arr, rows, cols, row_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_NORM39(arr, rows, cols, col_idx)` | RMS (root mean square) of values in the specified column |

### 9.116 String Encoding Functions (P426)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_BBBW_ENCODE(fields...)` | `:` | Encode fields into `:`-separated text with `\`-escaping |
| `STRING_BBBW_DECODE(line, idx)` | `:` | Decode field at index from `:`-separated text with unescaping |

### 9.117 Array Matrix Functions (P427)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM40(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV40(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.118 String Encoding Functions (P427)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_CCBW_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_CCBW_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.119 Array Matrix Functions (P428)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM41(arr, rows, cols, row_idx)` | L1 norm (sum of absolute values) of the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_DEV41(arr, rows, cols, col_idx)` | Population standard deviation of values in the specified column |

### 9.120 String Encoding Functions (P428)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_DDBW_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_DDBW_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.121 Array Matrix Functions (P429)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_DEV41(arr, rows, cols, row_idx)` | Population standard deviation of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_NORM41(arr, rows, cols, col_idx)` | L1 norm (sum of absolute values) of the specified column |

### 9.122 String Encoding Functions (P429)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_EEBW_ENCODE(fields...)` | `@` | Encode fields into `@`-separated text with `\`-escaping |
| `STRING_EEBW_DECODE(line, idx)` | `@` | Decode field at index from `@`-separated text with unescaping |

### 9.123 Array Matrix Functions (P430)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_DEV41(arr, rows, cols, row_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_DEV42(arr, rows, cols, col_idx)` | Population standard deviation of values in the specified column |

### 9.124 String Encoding Functions (P430)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_FFBW_ENCODE(fields...)` | `=` | Encode fields into `=`-separated text with `\`-escaping |
| `STRING_FFBW_DECODE(line, idx)` | `=` | Decode field at index from `=`-separated text with unescaping |

### 9.125 Array Matrix Functions (P431)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM42(arr, rows, cols, row_idx)` | L1 norm (sum of absolute values) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV42(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.126 String Encoding Functions (P431)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_GGBW_ENCODE(fields...)` | `%` | Encode fields into `%`-separated text with `\`-escaping |
| `STRING_GGBW_DECODE(line, idx)` | `%` | Decode field at index from `%`-separated text with unescaping |

### 9.127 Array Matrix Functions (P432)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_RANGE42(arr, rows, cols, row_idx)` | Range (max − min) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_DEV42(arr, rows, cols, col_idx)` | Mean absolute deviation of values in the specified column |

### 9.128 String Encoding Functions (P432)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_HHBW_ENCODE(fields...)` | `\|` | Encode fields into `\|`-separated text with `\`-escaping |
| `STRING_HHBW_DECODE(line, idx)` | `\|` | Decode field at index from `\|`-separated text with unescaping |

### 9.129 Array Matrix Functions (P433)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_DEV42(arr, rows, cols, row_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_NORM42(arr, rows, cols, col_idx)` | RMS (root mean square) of values in the specified column |

### 9.130 String Encoding Functions (P433)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_IIBW_ENCODE(fields...)` | `:` | Encode fields into `:`-separated text with `\`-escaping |
| `STRING_IIBW_DECODE(line, idx)` | `:` | Decode field at index from `:`-separated text with unescaping |

### 9.131 Array Matrix Functions (P434)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM43(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV43(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.132 String Encoding Functions (P434)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_JJBW_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_JJBW_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.133 Array Matrix Functions (P435)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_RANGE43(arr, rows, cols, row_idx)` | Range (max − min) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_DEV43(arr, rows, cols, col_idx)` | Mean absolute deviation of values in the specified column |

### 9.134 String Encoding Functions (P435)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_KKBW_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_KKBW_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.135 Array Matrix Functions (P436)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM43(arr, rows, cols, row_idx)` | L1 norm (sum of absolute values) of the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_DEV43(arr, rows, cols, col_idx)` | Population standard deviation of values in the specified column |

### 9.136 String Encoding Functions (P436)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_LLBW_ENCODE(fields...)` | `@` | Encode fields into `@`-separated text with `\`-escaping |
| `STRING_LLBW_DECODE(line, idx)` | `@` | Decode field at index from `@`-separated text with unescaping |

### 9.137 Array Matrix Functions (P437)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_DEV43(arr, rows, cols, row_idx)` | Population standard deviation of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_NORM43(arr, rows, cols, col_idx)` | L1 norm (sum of absolute values) of the specified column |

### 9.138 String Encoding Functions (P437)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_MMBW_ENCODE(fields...)` | `=` | Encode fields into `=`-separated text with `\`-escaping |
| `STRING_MMBW_DECODE(line, idx)` | `=` | Decode field at index from `=`-separated text with unescaping |

### 9.139 Array Matrix Functions (P438)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_DEV43(arr, rows, cols, row_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_DEV44(arr, rows, cols, col_idx)` | Population standard deviation of values in the specified column |

### 9.140 String Encoding Functions (P438)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_NNBW_ENCODE(fields...)` | `%` | Encode fields into `%`-separated text with `\`-escaping |
| `STRING_NNBW_DECODE(line, idx)` | `%` | Decode field at index from `%`-separated text with unescaping |

### 9.141 Array Matrix Functions (P439)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_DEV44(arr, rows, cols, row_idx)` | Mean absolute deviation of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV44(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.142 String Encoding Functions (P439)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_OOBW_ENCODE(fields...)` | `\|` | Encode fields into `\|`-separated text with `\`-escaping |
| `STRING_OOBW_DECODE(line, idx)` | `\|` | Decode field at index from `\|`-separated text with unescaping |

### 9.143 Array Matrix Functions (P440)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_RANGE44(arr, rows, cols, row_idx)` | Range (max − min) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_DEV44(arr, rows, cols, col_idx)` | Mean absolute deviation of values in the specified column |

### 9.144 String Encoding Functions (P440)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_PPBW_ENCODE(fields...)` | `:` | Encode fields into `:`-separated text with `\`-escaping |
| `STRING_PPBW_DECODE(line, idx)` | `:` | Decode field at index from `:`-separated text with unescaping |

### 9.145 Array Matrix Functions (P441)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM44(arr, rows, cols, row_idx)` | Sum of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_RANGE44(arr, rows, cols, col_idx)` | Range (max − min) of values in the specified column |

### 9.146 String Encoding Functions (P441)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_QQBW_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_QQBW_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.147 Array Matrix Functions (P442)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_DEV44(arr, rows, cols, row_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_NORM44(arr, rows, cols, col_idx)` | Sum of absolute values in the specified column |

### 9.148 String Encoding Functions (P442)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_RRBW_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_RRBW_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.149 Array Matrix Functions (P443)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM45(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV45(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.150 String Encoding Functions (P443)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_SSBW_ENCODE(fields...)` | `@` | Encode fields into `@`-separated text with `\`-escaping |
| `STRING_SSBW_DECODE(line, idx)` | `@` | Decode field at index from `@`-separated text with unescaping |

### 9.151 Array Matrix Functions (P444)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_RANGE45(arr, rows, cols, row_idx)` | Range (max − min) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_DEV45(arr, rows, cols, col_idx)` | Mean absolute deviation of values in the specified column |

### 9.152 String Encoding Functions (P444)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_TTBW_ENCODE(fields...)` | `=` | Encode fields into `=`-separated text with `\`-escaping |
| `STRING_TTBW_DECODE(line, idx)` | `=` | Decode field at index from `=`-separated text with unescaping |

### 9.153 Array Matrix Functions (P445)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM45(arr, rows, cols, row_idx)` | Sum of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_RANGE45(arr, rows, cols, col_idx)` | Range (max − min) of values in the specified column |

### 9.154 String Encoding Functions (P445)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_UUBW_ENCODE(fields...)` | `%` | Encode fields into `%`-separated text with `\`-escaping |
| `STRING_UUBW_DECODE(line, idx)` | `%` | Decode field at index from `%`-separated text with unescaping |

### 9.155 Array Matrix Functions (P446)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_DEV45(arr, rows, cols, row_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_NORM45(arr, rows, cols, col_idx)` | Sum of absolute values in the specified column |

### 9.156 String Encoding Functions (P446)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_VVBW_ENCODE(fields...)` | `\|` | Encode fields into `\|`-separated text with `\`-escaping |
| `STRING_VVBW_DECODE(line, idx)` | `\|` | Decode field at index from `\|`-separated text with unescaping |

### 9.157 Array Matrix Functions (P447)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM46(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV46(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.158 String Encoding Functions (P447)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_WWBW_ENCODE(fields...)` | `:` | Encode fields into `:`-separated text with `\`-escaping |
| `STRING_WWBW_DECODE(line, idx)` | `:` | Decode field at index from `:`-separated text with unescaping |

### 9.159 Array Matrix Functions (P448)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_DEV46(arr, rows, cols, row_idx)` | Mean absolute deviation of values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_NORM46(arr, rows, cols, col_idx)` | RMS (root mean square) of values in the specified column |

### 9.160 String Encoding Functions (P448)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_XXBW_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_XXBW_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.161 Array Matrix Functions (P449)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_RANGE46(arr, rows, cols, row_idx)` | Range (max − min) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_DEV46(arr, rows, cols, col_idx)` | Mean absolute deviation of values in the specified column |

### 9.162 String Encoding Functions (P449)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_YYBW_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_YYBW_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.163 Array Matrix Functions (P450)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM46(arr, rows, cols, row_idx)` | Mean of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_RANGE46(arr, rows, cols, col_idx)` | Range (max − min) of values in the specified column |

### 9.164 String Encoding Functions (P450)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_ZZBW_ENCODE(fields...)` | `@` | Encode fields into `@`-separated text with `\`-escaping |
| `STRING_ZZBW_DECODE(line, idx)` | `@` | Decode field at index from `@`-separated text with unescaping |

### 9.165 Array Matrix Functions (P451)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM47(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV47(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.166 String Encoding Functions (P451)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_AACW_ENCODE(fields...)` | `=` | Encode fields into `=`-separated text with `\`-escaping |
| `STRING_AACW_DECODE(line, idx)` | `=` | Decode field at index from `=`-separated text with unescaping |

### 9.167 Array Matrix Functions (P452)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_DEV47(arr, rows, cols, row_idx)` | Mean absolute deviation of values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_NORM47(arr, rows, cols, col_idx)` | RMS (root mean square) of values in the specified column |

### 9.168 String Encoding Functions (P452)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_BBCW_ENCODE(fields...)` | `%` | Encode fields into `%`-separated text with `\`-escaping |
| `STRING_BBCW_DECODE(line, idx)` | `%` | Decode field at index from `%`-separated text with unescaping |

### 9.169 Array Matrix Functions (P453)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_DEV47(arr, rows, cols, row_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_DEV47(arr, rows, cols, col_idx)` | Mean absolute deviation of values in the specified column |

### 9.170 String Encoding Functions (P453)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_CCCW_ENCODE(fields...)` | `\|` | Encode fields into `\|`-separated text with `\`-escaping |
| `STRING_CCCW_DECODE(line, idx)` | `\|` | Decode field at index from `\|`-separated text with unescaping |

### 9.171 Array Matrix Functions (P454)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_RANGE47(arr, rows, cols, row_idx)` | Range (max − min) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_NORM47(arr, rows, cols, col_idx)` | Mean of absolute values in the specified column |

### 9.172 String Encoding Functions (P454)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_DDCW_ENCODE(fields...)` | `:` | Encode fields into `:`-separated text with `\`-escaping |
| `STRING_DDCW_DECODE(line, idx)` | `:` | Decode field at index from `:`-separated text with unescaping |

### 9.173 Array Matrix Functions (P455)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM48(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV48(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.174 String Encoding Functions (P455)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_EECW_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_EECW_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.175 Array Matrix Functions (P456)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_RANGE48(arr, rows, cols, row_idx)` | Range (max − min) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_DEV48(arr, rows, cols, col_idx)` | Mean absolute deviation of values in the specified column |

### 9.176 String Encoding Functions (P456)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_FFCW_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_FFCW_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.177 Array Matrix Functions (P457)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_DEV48(arr, rows, cols, row_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_NORM48(arr, rows, cols, col_idx)` | RMS (root mean square) of values in the specified column |

### 9.178 String Encoding Functions (P457)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_GGCW_ENCODE(fields...)` | `@` | Encode fields into `@`-separated text with `\`-escaping |
| `STRING_GGCW_DECODE(line, idx)` | `@` | Decode field at index from `@`-separated text with unescaping |

### 9.179 Array Matrix Functions (P458)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM48(arr, rows, cols, row_idx)` | Mean of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE48(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.180 String Encoding Functions (P458)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_HHCW_ENCODE(fields...)` | `=` | Encode fields into `=`-separated text with `\`-escaping |
| `STRING_HHCW_DECODE(line, idx)` | `=` | Decode field at index from `=`-separated text with unescaping |

### 9.181 Array Matrix Functions (P459)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_RANGE49(arr, rows, cols, row_idx)` | Range (max − min) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_DEV49(arr, rows, cols, col_idx)` | Mean absolute deviation of values in the specified column |

### 9.182 String Encoding Functions (P459)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_IICW_ENCODE(fields...)` | `%` | Encode fields into `%`-separated text with `\`-escaping |
| `STRING_IICW_DECODE(line, idx)` | `%` | Decode field at index from `%`-separated text with unescaping |

### 9.183 Array Matrix Functions (P460)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM49(arr, rows, cols, row_idx)` | Mean of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE49(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.184 String Encoding Functions (P460)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_JJCW_ENCODE(fields...)` | `\|` | Encode fields into `\|`-separated text with `\`-escaping |
| `STRING_JJCW_DECODE(line, idx)` | `\|` | Decode field at index from `\|`-separated text with unescaping |

### 9.185 Array Matrix Functions (P461)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_DEV49(arr, rows, cols, row_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_NORM49(arr, rows, cols, col_idx)` | RMS (root mean square) of values in the specified column |

### 9.186 String Encoding Functions (P461)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_KKCW_ENCODE(fields...)` | `:` | Encode fields into `:`-separated text with `\`-escaping |
| `STRING_KKCW_DECODE(line, idx)` | `:` | Decode field at index from `:`-separated text with unescaping |

### 9.187 Array Matrix Functions (P462)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM49(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV49(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.188 String Encoding Functions (P462)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_LLCW_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_LLCW_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.189 Array Matrix Functions (P463)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM50(arr, rows, cols, row_idx)` | Mean of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE50(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.190 String Encoding Functions (P463)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_MMCW_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_MMCW_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.191 Array Matrix Functions (P464)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM50(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV50(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.192 String Encoding Functions (P464)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_NNCW_ENCODE(fields...)` | `@` | Encode fields into `@`-separated text with `\`-escaping |
| `STRING_NNCW_DECODE(line, idx)` | `@` | Decode field at index from `@`-separated text with unescaping |

### 9.193 Array Matrix Functions (P465)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_DEV50(arr, rows, cols, row_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_NORM50(arr, rows, cols, col_idx)` | RMS (root mean square) of values in the specified column |

### 9.194 String Encoding Functions (P465)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_OOCW_ENCODE(fields...)` | `=` | Encode fields into `=`-separated text with `\`-escaping |
| `STRING_OOCW_DECODE(line, idx)` | `=` | Decode field at index from `=`-separated text with unescaping |

### 9.195 Array Matrix Functions (P466)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_RANGE50(arr, rows, cols, row_idx)` | Range of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_NORM50(arr, rows, cols, col_idx)` | Mean of absolute values in the specified column |

### 9.196 String Encoding Functions (P466)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_PPCW_ENCODE(fields...)` | `%` | Encode fields into `%`-separated text with `\`-escaping |
| `STRING_PPCW_DECODE(line, idx)` | `%` | Decode field at index from `%`-separated text with unescaping |

### 9.197 Array Matrix Functions (P467)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM51(arr, rows, cols, row_idx)` | Mean of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_ABS_DEV51(arr, rows, cols, col_idx)` | Stddev of absolute values in the specified column |

### 9.198 String Encoding Functions (P467)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_QQCW_ENCODE(fields...)` | `\|` | Encode fields into `\|`-separated text with `\`-escaping |
| `STRING_QQCW_DECODE(line, idx)` | `\|` | Decode field at index from `\|`-separated text with unescaping |

### 9.199 Array Matrix Functions (P468)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_DEV51(arr, rows, cols, row_idx)` | Stddev of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE51(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.200 String Encoding Functions (P468)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_RRCW_ENCODE(fields...)` | `:` | Encode fields into `:`-separated text with `\`-escaping |
| `STRING_RRCW_DECODE(line, idx)` | `:` | Decode field at index from `:`-separated text with unescaping |

### 9.201 Array Matrix Functions (P469)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_LOG_DEV51(arr, rows, cols, row_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified row |
| `ARRAY_MATRIX_COLUMN_RMS_NORM51(arr, rows, cols, col_idx)` | RMS (root mean square) of values in the specified column |

### 9.202 String Encoding Functions (P469)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_SSCW_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_SSCW_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.203 Array Matrix Functions (P470)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM51(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV51(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.204 String Encoding Functions (P470)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_TTCW_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_TTCW_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.205 Array Matrix Functions (P471)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM52(arr, rows, cols, row_idx)` | Mean of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE52(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.206 String Encoding Functions (P471)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_UUCW_ENCODE(fields...)` | `@` | Encode fields into `@`-separated text with `\`-escaping |
| `STRING_UUCW_DECODE(line, idx)` | `@` | Decode field at index from `@`-separated text with unescaping |

### 9.207 Array Matrix Functions (P472)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM52(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV52(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.208 String Encoding Functions (P472)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_VVCW_ENCODE(fields...)` | `=` | Encode fields into `=`-separated text with `\`-escaping |
| `STRING_VVCW_DECODE(line, idx)` | `=` | Decode field at index from `=`-separated text with unescaping |

### 9.209 Array Matrix Functions (P473)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM53(arr, rows, cols, row_idx)` | Mean of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE53(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.210 String Encoding Functions (P473)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_WWCW_ENCODE(fields...)` | `%` | Encode fields into `%`-separated text with `\`-escaping |
| `STRING_WWCW_DECODE(line, idx)` | `%` | Decode field at index from `%`-separated text with unescaping |

### 9.211 Array Matrix Functions (P474)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM53(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV53(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.212 String Encoding Functions (P474)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_XXCW_ENCODE(fields...)` | `\|` | Encode fields into `\|`-separated text with `\`-escaping |
| `STRING_XXCW_DECODE(line, idx)` | `\|` | Decode field at index from `\|`-separated text with unescaping |

### 9.213 Array Matrix Functions (P475)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM54(arr, rows, cols, row_idx)` | Mean of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE54(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.214 String Encoding Functions (P475)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_YYCW_ENCODE(fields...)` | `:` | Encode fields into `:`-separated text with `\`-escaping |
| `STRING_YYCW_DECODE(line, idx)` | `:` | Decode field at index from `:`-separated text with unescaping |

### 9.215 Array Matrix Functions (P476)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM54(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV54(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.216 String Encoding Functions (P476)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_ZZCW_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_ZZCW_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.217 Array Matrix Functions (P477)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM55(arr, rows, cols, row_idx)` | Mean of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE55(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.218 String Encoding Functions (P477)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_AADW_ENCODE(fields...)` | `+` | Encode fields into `+`-separated text with `\`-escaping |
| `STRING_AADW_DECODE(line, idx)` | `+` | Decode field at index from `+`-separated text with unescaping |

### 9.219 Array Matrix Functions (P478)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM55(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV55(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.220 String Encoding Functions (P478)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_BBDW_ENCODE(fields...)` | `*` | Encode fields into `*`-separated text with `\`-escaping |
| `STRING_BBDW_DECODE(line, idx)` | `*` | Decode field at index from `*`-separated text with unescaping |

### 9.221 Array Matrix Functions (P479)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM56(arr, rows, cols, row_idx)` | Mean of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE56(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.222 String Encoding Functions (P479)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_CCDW_ENCODE(fields...)` | `#` | Encode fields into `#`-separated text with `\`-escaping |
| `STRING_CCDW_DECODE(line, idx)` | `#` | Decode field at index from `#`-separated text with unescaping |

### 9.223 Array Matrix Functions (P480)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM56(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV56(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.224 String Encoding Functions (P480)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_DDDW_ENCODE(fields...)` | `&` | Encode fields into `&`-separated text with `\`-escaping |
| `STRING_DDDW_DECODE(line, idx)` | `&` | Decode field at index from `&`-separated text with unescaping |

### 9.225 Array Matrix Functions (P481)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM57(arr, rows, cols, row_idx)` | Mean of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE57(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.226 String Encoding Functions (P481)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_EEDW_ENCODE(fields...)` | `!` | Encode fields into `!`-separated text with `\`-escaping |
| `STRING_EEDW_DECODE(line, idx)` | `!` | Decode field at index from `!`-separated text with unescaping |

### 9.227 Array Matrix Functions (P482)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM57(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV57(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.228 String Encoding Functions (P482)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_FFDW_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_FFDW_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.229 Array Matrix Functions (P483)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM58(arr, rows, cols, row_idx)` | Mean of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE58(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.230 String Encoding Functions (P483)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_GGDW_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_GGDW_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.231 Array Matrix Functions (P484)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM58(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV58(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.232 String Encoding Functions (P484)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_HHDW_ENCODE(fields...)` | `@` | Encode fields into `@`-separated text with `\`-escaping |
| `STRING_HHDW_DECODE(line, idx)` | `@` | Decode field at index from `@`-separated text with unescaping |

### 9.233 Array Matrix Functions (P485)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM59(arr, rows, cols, row_idx)` | Mean of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE59(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.234 String Encoding Functions (P485)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_IIDW_ENCODE(fields...)` | `=` | Encode fields into `=`-separated text with `\`-escaping |
| `STRING_IIDW_DECODE(line, idx)` | `=` | Decode field at index from `=`-separated text with unescaping |

### 9.235 Array Matrix Functions (P486)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM59(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV59(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.236 String Encoding Functions (P486)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_JJDW_ENCODE(fields...)` | `%` | Encode fields into `%`-separated text with `\`-escaping |
| `STRING_JJDW_DECODE(line, idx)` | `%` | Decode field at index from `%`-separated text with unescaping |

### 9.237 Array Matrix Functions (P487)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM60(arr, rows, cols, row_idx)` | Mean of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE60(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.238 String Encoding Functions (P487)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_KKDW_ENCODE(fields...)` | `\|` | Encode fields into `\|`-separated text with `\`-escaping |
| `STRING_KKDW_DECODE(line, idx)` | `\|` | Decode field at index from `\|`-separated text with unescaping |

### 9.239 Array Matrix Functions (P488)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM60(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV60(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.240 String Encoding Functions (P488)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_LLDW_ENCODE(fields...)` | `:` | Encode fields into `:`-separated text with `\`-escaping |
| `STRING_LLDW_DECODE(line, idx)` | `:` | Decode field at index from `:`-separated text with unescaping |

### 9.241 Array Matrix Functions (P489)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM61(arr, rows, cols, row_idx)` | Mean of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE61(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.242 String Encoding Functions (P489)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_MMDW_ENCODE(fields...)` | `+` | Encode fields into `+`-separated text with `\`-escaping |
| `STRING_MMDW_DECODE(line, idx)` | `+` | Decode field at index from `+`-separated text with unescaping |

### 9.243 Array Matrix Functions (P490)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM61(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV61(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.244 String Encoding Functions (P490)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_NNDW_ENCODE(fields...)` | `*` | Encode fields into `*`-separated text with `\`-escaping |
| `STRING_NNDW_DECODE(line, idx)` | `*` | Decode field at index from `*`-separated text with unescaping |

### 9.245 Array Matrix Functions (P491)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM62(arr, rows, cols, row_idx)` | Mean of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE62(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.246 String Encoding Functions (P491)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_OODW_ENCODE(fields...)` | `#` | Encode fields into `#`-separated text with `\`-escaping |
| `STRING_OODW_DECODE(line, idx)` | `#` | Decode field at index from `#`-separated text with unescaping |

### 9.247 Array Matrix Functions (P492)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM62(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV62(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.248 String Encoding Functions (P492)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_PPDW_ENCODE(fields...)` | `&` | Encode fields into `&`-separated text with `\`-escaping |
| `STRING_PPDW_DECODE(line, idx)` | `&` | Decode field at index from `&`-separated text with unescaping |

### 9.249 Array Matrix Functions (P493)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM63(arr, rows, cols, row_idx)` | Mean of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE63(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.250 String Encoding Functions (P493)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_QQDW_ENCODE(fields...)` | `!` | Encode fields into `!`-separated text with `\`-escaping |
| `STRING_QQDW_DECODE(line, idx)` | `!` | Decode field at index from `!`-separated text with unescaping |

### 9.251 Array Matrix Functions (P494)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM63(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV63(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.252 String Encoding Functions (P494)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_RRDW_ENCODE(fields...)` | `~` | Encode fields into `~`-separated text with `\`-escaping |
| `STRING_RRDW_DECODE(line, idx)` | `~` | Decode field at index from `~`-separated text with unescaping |

### 9.253 Array Matrix Functions (P495)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM64(arr, rows, cols, row_idx)` | Mean of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE64(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.254 String Encoding Functions (P495)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_SSDW_ENCODE(fields...)` | `^` | Encode fields into `^`-separated text with `\`-escaping |
| `STRING_SSDW_DECODE(line, idx)` | `^` | Decode field at index from `^`-separated text with unescaping |

### 9.255 Array Matrix Functions (P496)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM64(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV64(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.256 String Encoding Functions (P496)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_TTDW_ENCODE(fields...)` | `@` | Encode fields into `@`-separated text with `\`-escaping |
| `STRING_TTDW_DECODE(line, idx)` | `@` | Decode field at index from `@`-separated text with unescaping |

### 9.257 Array Matrix Functions (P497)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_ABS_NORM65(arr, rows, cols, row_idx)` | Mean of absolute values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_RANGE65(arr, rows, cols, col_idx)` | Range of ln(\|v\|) for non-zero values in the specified column |

### 9.258 String Encoding Functions (P497)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_UUDW_ENCODE(fields...)` | `=` | Encode fields into `=`-separated text with `\`-escaping |
| `STRING_UUDW_DECODE(line, idx)` | `=` | Decode field at index from `=`-separated text with unescaping |

### 9.259 Array Matrix Functions (P498)

| Function | Description |
|----------|-------------|
| `ARRAY_MATRIX_ROW_RMS_NORM65(arr, rows, cols, row_idx)` | RMS (root mean square) of values in the specified row |
| `ARRAY_MATRIX_COLUMN_LOG_DEV65(arr, rows, cols, col_idx)` | Stddev of ln(\|v\|) for non-zero values in the specified column |

### 9.260 String Encoding Functions (P498)

| Function | Delimiter | Description |
|----------|-----------|-------------|
| `STRING_VVDW_ENCODE(fields...)` | `%` | Encode fields into `%`-separated text with `\`-escaping |
| `STRING_VVDW_DECODE(line, idx)` | `%` | Decode field at index from `%`-separated text with unescaping |