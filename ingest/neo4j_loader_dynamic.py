# ingest/neo4j_loader_dynamic.py
# Usage (Windows PowerShell one-line):
#   python .\ingest\neo4j_loader_dynamic.py --excel "C:\Path\Hackathon_Master_Database.xlsx" --sheet "Master Data - CL" --uri "neo4j+s://210ee072.databases.neo4j.io" --user "neo4j" --password "YOUR_AURA_PASSWORD" --database "neo4j"
#
# What it does:
# - Reads your Excel sheet
# - Creates/updates Employee, Skill, Project (+ optional Team) nodes
# - Adds REPORTS_TO, LEAD_REPORTING, HAS_SKILL, WORKS_ON, WORKS_IN relationships
# - Parses mixed date formats gracefully
# - Attaches any unknown Excel columns as Employee properties (snake_case) WITHOUT APOC

import argparse
import pandas as pd
from neo4j import GraphDatabase
from datetime import date
import re
import sys
from typing import Dict, Any, List

# --------------------------
# Helpers
# --------------------------

def norm_col(c: str) -> str:
    """Normalize column header whitespace."""
    return re.sub(r"\s+", " ", c.strip())

def norm_str(x):
    """Return trimmed string or None."""
    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    s = str(x).strip()
    return s if s else None

def snake(s: str) -> str:
    """snake_case a header into a property key."""
    s = s.strip().replace("(", "").replace(")", "")
    s = re.sub(r"[^0-9A-Za-z]+", "_", s).strip("_")
    return s.lower()

def parse_date_flex(x):
    """
    Parse many date shapes (DDMMYY, DD/MM/YY, DD-MM-YYYY, YYYY-MM-DD, Excel datetime).
    Returns ISO 'YYYY-MM-DD' or None.
    """
    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass

    s = str(x).strip()
    if not s:
        return None

    # Try pandas flexible parser (suppress the warning)
    for dayfirst in (True, False):
        try:
            dt = pd.to_datetime(s, dayfirst=dayfirst, errors="coerce")
            if pd.notna(dt):
                return dt.date().isoformat()
        except Exception:
            pass

    # Try DDMMYY without separators
    s2 = s.replace("/", "").replace("-", "").replace(".", "").replace(" ", "")
    if len(s2) == 6 and s2.isdigit():
        dd, mm, yy = int(s2[:2]), int(s2[2:4]), int(s2[4:])
        # standard 2-digit year window
        year = 2000 + yy if yy <= 69 else 1900 + yy
        try:
            return date(year, mm, dd).isoformat()
        except Exception:
            return None

    return None

# --------------------------
# Record builder
# --------------------------

def build_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Normalize Excel data and map known columns to canonical fields.
    Unknown columns go into 'extra' and will be attached to Employee via SET e += r.extra
    """
    col_map_variants = {
        "emp_id": ["Emp Id", "Emp Id ", "Employee Id", "EmpID"],
        "name": ["Emp Name", "Employee Name", "Name"],
        "gender": ["Gender"],
        "doj": ["Date of Joining (DDMMYY)", "Date of Joining", "DOJ"],
        "designation": ["Designation", "Title", "Role"],
        "manager_name": ["Reporting Manager", "Manager", "Reports To"],
        "lead_name": ["Lead Reporting", "Lead", "Project Lead"],
        "primary_skill": ["Primary Skill", "Primary skill"],
        "secondary_skill": ["Secondary Skill", "Secondary skill"],
        "project": ["Current Project", "Project"],
        # Optional (if/when you add it)
        "team": ["Team", "Department"]
    }

    df = df.copy()
    df.columns = [norm_col(c) for c in df.columns]

    # Invert header mapping
    inv = {}
    for key, variants in col_map_variants.items():
        for v in variants:
            inv[norm_col(v)] = key

    records: List[Dict[str, Any]] = []

    # Columns not recognized become passthrough properties
    passthrough_cols = set(df.columns) - set(inv.keys())
    ignore_cols = {"Sr No", "S.No", "Sr. No", "Serial", "Serial No"}
    passthrough_cols = {c for c in passthrough_cols if norm_col(c) not in ignore_cols}

    for _, row in df.iterrows():
        rec: Dict[str, Any] = {
            "empId": None,
            "name": None,
            "gender": None,
            "doj": None,
            "designation": None,
            "managerName": None,
            "leadName": None,
            "primarySkill": None,
            "secondarySkill": None,
            "project": None,
            "team": None,
            "extra": {}
        }

        for c, v in row.items():
            key = inv.get(norm_col(c))
            if key is None:
                val = norm_str(v)
                if val is not None:
                    rec["extra"][snake(c)] = val
            else:
                if key == "emp_id":
                    rec["empId"] = norm_str(v)
                elif key == "name":
                    rec["name"] = norm_str(v)
                elif key == "gender":
                    rec["gender"] = norm_str(v)
                elif key == "doj":
                    rec["doj"] = parse_date_flex(v)
                elif key == "designation":
                    rec["designation"] = norm_str(v)
                elif key == "manager_name":
                    rec["managerName"] = norm_str(v)
                elif key == "lead_name":
                    rec["leadName"] = norm_str(v)
                elif key == "primary_skill":
                    vs = norm_str(v)
                    rec["primarySkill"] = vs.lower() if vs else None
                elif key == "secondary_skill":
                    vs = norm_str(v)
                    rec["secondarySkill"] = vs.lower() if vs else None
                elif key == "project":
                    rec["project"] = norm_str(v)
                elif key == "team":
                    rec["team"] = norm_str(v)

        # Only keep rows with both Emp Id + Emp Name
        if rec["empId"] and rec["name"]:
            records.append(rec)

    return records

# --------------------------
# Cypher setup (run individually)
# --------------------------

SETUP_STATEMENTS = [
    "CREATE CONSTRAINT emp_id IF NOT EXISTS FOR (e:Employee) REQUIRE e.empId IS UNIQUE",
    "CREATE CONSTRAINT skill_name IF NOT EXISTS FOR (s:Skill) REQUIRE s.name IS UNIQUE",
    "CREATE CONSTRAINT project_name IF NOT EXISTS FOR (p:Project) REQUIRE p.name IS UNIQUE",
    "CREATE INDEX emp_name IF NOT EXISTS FOR (e:Employee) ON (e.name)",
    "CREATE INDEX team_name IF NOT EXISTS FOR (t:Team) ON (t.name)",
]

# --------------------------
# Upsert logic
# --------------------------

def upsert(driver, records: List[Dict[str, Any]], database: str = "neo4j"):
    if not records:
        print("⚠️  No valid rows (need Emp Id and Emp Name). Nothing to import.")
        return

    with driver.session(database=database) as s:
        # Run setup one-by-one (driver requires single statement per run)
        for stmt in SETUP_STATEMENTS:
            try:
                s.run(stmt)
            except Exception as e:
                # Non-fatal if already exists or lacks privilege; continue
                print(f"ℹ️  Setup statement notice: {stmt} -> {e}")

        # Employees + dynamic extra properties (APOC-free)
        s.run("""
        UNWIND $rows AS r
        MERGE (e:Employee {empId:r.empId})
          ON CREATE SET e.name = r.name
          ON MATCH  SET e.name = coalesce(r.name, e.name)
        SET e.gender = r.gender,
            e.designation = r.designation,
            e.doj = CASE WHEN r.doj IS NULL THEN e.doj ELSE date(r.doj) END,
            e += r.extra
        """, rows=records)

        # Optional Team link
        s.run("""
        UNWIND $rows AS r
        WITH r WHERE r.team IS NOT NULL AND r.team <> ""
        MATCH (e:Employee {empId:r.empId})
        MERGE (t:Team {name:r.team})
        MERGE (e)-[:WORKS_IN]->(t)
        """, rows=records)

        # Manager edges - FIXED VERSION
        s.run("""
        UNWIND $rows AS r
        WITH r WHERE r.managerName IS NOT NULL AND r.managerName <> ""
        MATCH (e:Employee {empId:r.empId})
        // Ensure manager exists (create stub if needed)
        MERGE (mgr:Employee {name:r.managerName})
          ON CREATE SET mgr.empId = "stub::manager::" + r.empId + "::" + r.managerName
        MERGE (e)-[:REPORTS_TO]->(mgr)
        """, rows=records)

        # Lead edges - FIXED VERSION
        s.run("""
        UNWIND $rows AS r
        WITH r WHERE r.leadName IS NOT NULL AND r.leadName <> ""
        MATCH (e:Employee {empId:r.empId})
        // Ensure lead exists (create stub if needed)
        MERGE (lead:Employee {name:r.leadName})
          ON CREATE SET lead.empId = "stub::lead::" + r.empId + "::" + r.leadName
        MERGE (e)-[:LEAD_REPORTING]->(lead)
        """, rows=records)

        # Skills
        s.run("""
        UNWIND $rows AS r
        MATCH (e:Employee {empId:r.empId})
        FOREACH (_ IN CASE WHEN r.primarySkill IS NULL THEN [] ELSE [1] END |
          MERGE (s1:Skill {name:r.primarySkill})
          MERGE (e)-[:HAS_SKILL {type:"primary"}]->(s1)
        )
        FOREACH (_ IN CASE WHEN r.secondarySkill IS NULL THEN [] ELSE [1] END |
          MERGE (s2:Skill {name:r.secondarySkill})
          MERGE (e)-[:HAS_SKILL {type:"secondary"}]->(s2)
        )
        """, rows=records)

        # Projects
        s.run("""
        UNWIND $rows AS r
        WITH r WHERE r.project IS NOT NULL AND r.project <> ""
        MATCH (e:Employee {empId:r.empId})
        MERGE (p:Project {name:r.project})
        MERGE (e)-[w:WORKS_ON]->(p)
        SET w.since = coalesce(w.since, CASE WHEN r.doj IS NULL THEN NULL ELSE date(r.doj) END)
        """, rows=records)

# --------------------------
# Main
# --------------------------

def main():
    ap = argparse.ArgumentParser(description="Import Excel employee data into Neo4j (Aura compatible, APOC-free).")
    ap.add_argument("--excel", required=True, help="Path to Excel file")
    ap.add_argument("--sheet", default="Master Data - CL", help="Sheet name (exact)")
    ap.add_argument("--uri", required=True, help="Neo4j connection URI, e.g., neo4j+s://<host>.databases.neo4j.io")
    ap.add_argument("--user", required=True, help="Neo4j username (e.g., neo4j)")
    ap.add_argument("--password", required=True, help="Neo4j password")
    ap.add_argument("--database", default="neo4j", help="Neo4j database (Aura default = neo4j)")
    args = ap.parse_args()

    # Load Excel
    try:
        df = pd.read_excel(args.excel, sheet_name=args.sheet, dtype=str)
    except ValueError as e:
        print(f"❌ Could not open sheet '{args.sheet}'. Error: {e}")
        print("   Tip: check the sheet name in Excel and pass it via --sheet \"Exact Name\"")
        sys.exit(1)
    except FileNotFoundError:
        print(f"❌ Excel file not found: {args.excel}")
        sys.exit(1)

    # Build records
    records = build_records(df)
    if not records:
        print("⚠️  No valid rows found (need both 'Emp Id' and 'Emp Name'). Please check your Excel.")
        sys.exit(0)

    # Connect + upsert
    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    try:
        upsert(driver, records, database=args.database)
    finally:
        driver.close()

    print(f"✅ Upserted {len(records)} employees (plus relationships).")

if __name__ == "__main__":
    main()