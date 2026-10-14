"""
lib/schema_guard.py - Schema Guard & Automated NOPI DDL Synchronizer

Performs in-memory header inspection (including streaming from .zip archives),
compares extracted columns against Teradata Vantage DBC.ColumnsV, and automatically
generates and executes NOPI VARCHAR(4000) table recreation upon schema drift.
"""

import csv
import gzip
import io
import logging
import re
import zipfile

logger = logging.getLogger("BICC_INTAKE_CONTROLLER")

TECHNICAL_AUDIT_COLUMNS = {"INFA_BATCH_ID", "INFA_LAST_REPLICATED"}


def clean_column_name(col_raw: str) -> str:
    """
    Sanitizes raw CSV column header strings into valid Teradata identifiers:
    - Trims whitespace and quotes
    - Converts non-alphanumeric characters to underscores
    - Deduplicates consecutive underscores
    - Converts to UPPERCASE
    """
    col = col_raw.strip().strip('"').strip("'")
    col = re.sub(r"[^A-Za-z0-9_]", "_", col)
    col = re.sub(r"_+", "_", col).strip("_")
    return col.upper()


def extract_csv_columns(file_path: str) -> list[str]:
    """
    Reads the header line (line 1) from a CSV file or archive (.zip, .gz)
    and returns an ordered list of sanitized column names.
    Supports comma, semicolon, and tab delimiters.
    Streams directly from memory for archives without writing uncompressed files to disk.
    """
    first_line = ""
    lower_path = file_path.lower()

    if lower_path.endswith(".zip"):
        with zipfile.ZipFile(file_path, "r") as z:
            csv_members = [n for n in z.namelist() if n.lower().endswith(".csv")]
            if not csv_members:
                raise ValueError(f"No CSV file found inside ZIP archive '{file_path}'.")
            with z.open(csv_members[0], "r") as f:
                first_line = io.TextIOWrapper(f, encoding="utf-8", errors="replace").readline().strip()
    elif lower_path.endswith(".gz"):
        with gzip.open(file_path, "rt", encoding="utf-8", errors="replace") as f:
            first_line = f.readline().strip()
    else:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            first_line = f.readline().strip()

    if not first_line:
        raise ValueError(f"File '{file_path}' has an empty header line.")

    # Delimiter auto-detection
    if ";" in first_line:
        delimiter = ";"
    elif "\t" in first_line:
        delimiter = "\t"
    else:
        delimiter = ","

    reader = csv.reader([first_line], delimiter=delimiter)
    raw_columns = next(reader)

    clean_cols = [clean_column_name(c) for c in raw_columns if clean_column_name(c)]
    if not clean_cols:
        raise ValueError(f"Could not extract any valid columns from header of '{file_path}'.")

    return clean_cols


def load_entity_mappings(mapping_file: str, filter_source: str = "ORACLE_FUSION") -> list[dict]:
    """
    Loads entity mapping rules for the specified source from entity_mapping.csv.
    Returns a list of dicts: [{"target_table": ..., "signature": ..., "extension": ...}]
    """
    rules = []
    with open(mapping_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            normalized = {k.strip().upper(): v.strip() for k, v in row.items() if k and v}
            tgt = normalized.get("TARGET_TABLE", "")
            sig = normalized.get("FILE_SIGNATURE", "")
            ext = normalized.get("EXTENSION", ".csv")
            src = normalized.get("SOURCE", "")

            if tgt and sig:
                # Match source filter (* matches all)
                if not src or src == "*" or filter_source.upper() in [s.strip().upper() for s in src.split(",")]:
                    rules.append({
                        "target_table": tgt,
                        "signature": sig.lower(),
                        "extension": ext.lower()
                    })

    return rules


def match_target_table_for_file(filename: str, mapping_rules: list[dict]) -> str | None:
    """
    Matches a given data filename against loaded mapping rules to resolve the target staging table.
    Supports single or comma-separated extensions (e.g. '.csv,.zip').
    """
    fn_lower = filename.lower()
    for rule in mapping_rules:
        if rule["signature"] in fn_lower:
            ext = rule["extension"]
            if not ext or ext == "*" or fn_lower.endswith(ext) or any(fn_lower.endswith(e.strip()) for e in ext.split(",")):
                return rule["target_table"]
    return None


def get_teradata_table_columns(connection, target_db: str, target_table: str) -> list[str] | None:
    """
    Queries DBC.ColumnsV for existing columns in the target table.
    Returns an ordered list of uppercase column names, excluding technical audit columns.
    Returns None if the table does not exist.
    """
    query = """
    SELECT TRIM(ColumnName)
    FROM DBC.ColumnsV
    WHERE DatabaseName = ?
      AND TableName = ?
    ORDER BY ColumnId
    """
    cursor = connection.cursor()
    try:
        cursor.execute(query, [target_db, target_table])
        rows = cursor.fetchall()
        if not rows:
            return None

        td_cols = []
        for r in rows:
            c_name = str(r[0]).strip().upper()
            if c_name not in TECHNICAL_AUDIT_COLUMNS:
                td_cols.append(c_name)

        return td_cols
    except Exception as e:
        logger.warning(f"Error checking DBC.ColumnsV for {target_db}.{target_table}: {e}")
        return None
    finally:
        cursor.close()


def generate_staging_ddl(target_db: str, target_table: str, columns: list[str], default_type: str = "VARCHAR(4000)") -> str:
    """
    Generates Teradata DDL for a NOPI staging table with all payload columns
    typed as VARCHAR(4000) and standard audit columns appended.
    """
    cols_sql = [f"    {c} {default_type} CHARACTER SET UNICODE NOT CASESPECIFIC" for c in columns]
    cols_sql.append("    INFA_LAST_REPLICATED TIMESTAMP(0)")
    cols_sql.append("    INFA_BATCH_ID VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC")

    cols_joined = ",\n".join(cols_sql)
    ddl = f"""CREATE MULTISET TABLE {target_db}.{target_table} ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
{cols_joined}
     )
NO PRIMARY INDEX;"""

    return ddl


def recreate_staging_table(connection, target_db: str, target_table: str, columns: list[str], default_type: str = "VARCHAR(4000)"):
    """
    Executes DROP TABLE (if table exists) followed by CREATE MULTISET TABLE ... NO PRIMARY INDEX.
    """
    cursor = connection.cursor()
    ddl = generate_staging_ddl(target_db, target_table, columns, default_type)

    # 1. Drop existing table
    drop_sql = f"DROP TABLE {target_db}.{target_table};"
    try:
        cursor.execute(drop_sql)
        logger.info(f"[{target_table}] Dropped existing table {target_db}.{target_table}")
    except Exception as e:
        # Error 3807: Table does not exist, safe to ignore
        err_msg = str(e)
        if "3807" in err_msg or "does not exist" in err_msg.lower():
            pass
        else:
            logger.warning(f"[{target_table}] Notice during DROP TABLE: {e}")

    # 2. Create new table
    try:
        cursor.execute(ddl)
        logger.info(f"[{target_table}] Successfully created NOPI staging table with {len(columns)} payload columns.")
    finally:
        cursor.close()


def sync_entity_schema_if_drifted(connection, target_db: str, target_table: str, sample_file_path: str, default_type: str = "VARCHAR(4000)") -> tuple[bool, str, list[str]]:
    """
    Compares CSV header columns in sample_file_path with the Teradata staging table.
    If the table does not exist or columns have drifted:
      Recreates the table using generate_staging_ddl.
    Returns:
      (is_identical: bool, action: str, columns: list[str])
    """
    csv_cols = extract_csv_columns(sample_file_path)
    td_cols = get_teradata_table_columns(connection, target_db, target_table)

    if td_cols is None:
        logger.info(f"[{target_table}] Table does not exist in Teradata staging. Creating table...")
        recreate_staging_table(connection, target_db, target_table, csv_cols, default_type)
        return False, "CREATED", csv_cols

    if csv_cols == td_cols:
        logger.info(f"[{target_table}] Schema is identical ({len(csv_cols)} columns). No DDL modification needed.")
        return True, "IDENTICAL", csv_cols

    logger.warning(
        f"[{target_table}] Schema drift detected!\n"
        f"  File columns ({len(csv_cols)}): {csv_cols}\n"
        f"  Table columns ({len(td_cols)}): {td_cols}"
    )
    recreate_staging_table(connection, target_db, target_table, csv_cols, default_type)
    return False, "RECREATED", csv_cols
