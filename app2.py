# app.py
import streamlit as st
import pandas as pd
import pymysql, requests

# ---------- DB + API ----------
HOST = "localhost"
USER = "root"
PASSWORD = ""
DB_NAME = "harvard_artifacts"
API_KEY = "20f6731f-61df-45d8-9a01-b48944b6ec56"
BASE_URL = "https://api.harvardartmuseums.org"

# ---------- DB Connection ----------
def get_conn():
    return pymysql.connect(host=HOST, user=USER, password=PASSWORD, database=DB_NAME)

# ---------- ETL ----------
def fetch_artifacts(classification="Paintings", size=100, page=1):
    url = f"{BASE_URL}/object"
    params = {"apikey": API_KEY, "classification": classification, "size": size, "page": page}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("records", [])

def transform_records(records):
    metadata_list, media_list, color_list = [], [], []
    for obj in records:
        oid = obj.get("objectid")
        if not oid:
            continue
        metadata_list.append((oid, obj.get("title"), obj.get("culture"), obj.get("period"),
                              obj.get("century"), obj.get("medium"), obj.get("dimensions"),
                              obj.get("description"), obj.get("department"),
                              obj.get("classification"), obj.get("accessionyear"),
                              obj.get("accessionmethod")))
        media_list.append((oid, obj.get("imagecount"), obj.get("mediacount"),
                           obj.get("colorcount"), obj.get("rank"),
                           obj.get("datebegin"), obj.get("dateend")))
        for c in obj.get("colors", []) or []:
            color_list.append((oid, c.get("color"), c.get("spectrum"),
                               c.get("hue"), c.get("percent"), c.get("css3")))
    return metadata_list, media_list, color_list

def insert_into_db(metadata, media, colors):
    conn = get_conn(); cur = conn.cursor()
    SQL_META = """INSERT IGNORE INTO artifact_metadata
    (id,title,culture,period,century,medium,dimensions,description,
     department,classification,accessionyear,accessionmethod)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    SQL_MEDIA = """INSERT IGNORE INTO artifact_media
    (objectid,imagecount,mediacount,colorcount,rank,datebegin,dateend)
    VALUES (%s,%s,%s,%s,%s,%s,%s)"""
    SQL_COLOR = """INSERT IGNORE INTO artifact_colors
    (objectid,color,spectrum,hue,percent,css3)
    VALUES (%s,%s,%s,%s,%s,%s)"""
    if metadata: cur.executemany(SQL_META, metadata)
    if media: cur.executemany(SQL_MEDIA, media)
    if colors: cur.executemany(SQL_COLOR, colors)
    conn.commit(); cur.close(); conn.close()

# ---------- Run Query Utility ----------
def run_query(sql, params=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params or ())
    results = cur.fetchall()
    cur.close(); conn.close()
    return results

# ---------- 20 Predefined Queries ----------
def q1_byzantine_11th_century():
    return run_query("SELECT * FROM artifact_metadata WHERE century='11th century' AND culture='Byzantine'")

def q2_unique_cultures():
    return run_query("SELECT DISTINCT culture FROM artifact_metadata")

def q3_archaic_period():
    return run_query("SELECT * FROM artifact_metadata WHERE period='Archaic'")

def q4_titles_desc_year():
    return run_query("SELECT title, accessionyear FROM artifact_metadata ORDER BY accessionyear DESC")

def q5_count_per_department():
    return run_query("SELECT department, COUNT(*) FROM artifact_metadata GROUP BY department")

def q6_more_than_3_images():
    return run_query("SELECT objectid, imagecount FROM artifact_media WHERE imagecount > 3")

def q7_avg_rank():
    return run_query("SELECT AVG(rank) FROM artifact_media")

def q8_colorcount_gt_mediacount():
    return run_query("SELECT objectid FROM artifact_media WHERE colorcount > mediacount")

def q9_created_1500_1600():
    return run_query("SELECT objectid, datebegin, dateend FROM artifact_media WHERE datebegin >= 1500 AND dateend <= 1600")

def q10_no_media():
    return run_query("SELECT objectid FROM artifact_media WHERE mediacount=0 OR mediacount IS NULL")

def q11_distinct_hues():
    return run_query("SELECT DISTINCT hue FROM artifact_colors")

def q12_top5_colors():
    return run_query("SELECT color, COUNT(*) as freq FROM artifact_colors GROUP BY color ORDER BY freq DESC LIMIT 5")

def q13_avg_percent_per_hue():
    return run_query("SELECT hue, AVG(percent) FROM artifact_colors GROUP BY hue")

def q14_colors_for_artifact(artifact_id):
    return run_query("SELECT color, hue FROM artifact_colors WHERE objectid=%s", (artifact_id,))

def q15_total_colors():
    return run_query("SELECT COUNT(*) FROM artifact_colors")

def q16_titles_hues_byzantine():
    return run_query("""SELECT m.title, c.hue
                        FROM artifact_metadata m
                        JOIN artifact_colors c ON m.id = c.objectid
                        WHERE m.culture='Byzantine'""")

def q17_titles_with_hues():
    return run_query("""SELECT m.title, c.hue
                        FROM artifact_metadata m
                        JOIN artifact_colors c ON m.id = c.objectid""")

def q18_titles_cultures_ranks():
    return run_query("""SELECT m.title, m.culture, a.rank
                        FROM artifact_metadata m
                        JOIN artifact_media a ON m.id = a.objectid
                        WHERE m.period IS NOT NULL""")

def q19_top10_grey():
    return run_query("""SELECT m.title
                        FROM artifact_metadata m
                        JOIN artifact_media a ON m.id = a.objectid
                        JOIN artifact_colors c ON m.id = c.objectid
                        WHERE c.hue='Grey'
                        ORDER BY a.rank DESC
                        LIMIT 10""")

def q20_artifacts_per_classification():
    return run_query("""SELECT m.classification, COUNT(*) as total, AVG(a.mediacount)
                        FROM artifact_metadata m
                        JOIN artifact_media a ON m.id = a.objectid
                        GROUP BY m.classification""")

# ---------- Streamlit UI ----------
st.title("Harvard’s Artifacts Collection")

classification = st.selectbox("Select Classification:", ["Paintings","Coins","Drawings","Sculpture","Prints"])
pages = st.slider("Pages to fetch (×100 per page)", 1, 40, 25)  # 25 pages ≈ 2500 records
size  = 100  # API max

if "fetched_data" not in st.session_state: st.session_state.fetched_data = None
if "transformed" not in st.session_state: st.session_state.transformed = None

if st.button("Collect Data"):
    all_records = []
    for page in range(1, pages + 1):
        records = fetch_artifacts(classification, size=size, page=page)
        all_records.extend(records)
    st.session_state.fetched_data = all_records
    st.success(f"Collected {len(all_records)} records")

if st.button("Show Data"):
    if st.session_state.fetched_data:
        meta, media, colors = transform_records(st.session_state.fetched_data)
        st.session_state.transformed = (meta, media, colors)
        st.info(f"Prepared for insert → metadata: {len(meta)}, media: {len(media)}, colors: {len(colors)}")
        st.write("### Metadata Sample"); st.dataframe(pd.DataFrame(meta).head())
        st.write("### Media Sample");    st.dataframe(pd.DataFrame(media).head())
        st.write("### Colors Sample");   st.dataframe(pd.DataFrame(colors).head())
    else:
        st.warning("Collect data first!")

if st.button("Insert into SQL"):
    if st.session_state.transformed:
        meta, media, colors = st.session_state.transformed
        insert_into_db(meta, media, colors)
        st.success(f"Inserted into MySQL → metadata: {len(meta)}, media: {len(media)}, colors: {len(colors)}")
    else:
        st.warning("Nothing to insert!")

# --- Predefined Queries UI ---
st.subheader("Run Predefined Queries")

artifact_id_for_q14 = st.number_input("Artifact ID for 'Colors for Artifact'", min_value=1, value=1, step=1)

query_map = {
    "11th Century Byzantine Artifacts": (q1_byzantine_11th_century, ["ID","Title","Culture","Period","Century","Medium","Dimensions","Description","Department","Classification","AccessionYear","AccessionMethod"]),
    "Unique Cultures": (q2_unique_cultures, ["Culture"]),
    "Archaic Period Artifacts": (q3_archaic_period, ["ID","Title","Culture","Period","Century","Medium","Dimensions","Description","Department","Classification","AccessionYear","AccessionMethod"]),
    "Titles Descending by Year": (q4_titles_desc_year, ["Title","AccessionYear"]),
    "Artifacts per Department": (q5_count_per_department, ["Department","Count"]),
    "Artifacts with >3 Images": (q6_more_than_3_images, ["ObjectID","ImageCount"]),
    "Average Rank": (q7_avg_rank, ["AvgRank"]),
    "ColorCount > MediaCount": (q8_colorcount_gt_mediacount, ["ObjectID"]),
    "Created 1500-1600": (q9_created_1500_1600, ["ObjectID","DateBegin","DateEnd"]),
    "Artifacts with No Media": (q10_no_media, ["ObjectID"]),
    "Distinct Hues": (q11_distinct_hues, ["Hue"]),
    "Top 5 Colors": (q12_top5_colors, ["Color","Frequency"]),
    "Average Percent per Hue": (q13_avg_percent_per_hue, ["Hue","AvgPercent"]),
    "Colors for Artifact (choose ID above)": (lambda: q14_colors_for_artifact(artifact_id_for_q14), ["Color","Hue"]),
    "Total Colors": (q15_total_colors, ["TotalColors"]),
    "Titles & Hues of Byzantine Artifacts": (q16_titles_hues_byzantine, ["Title","Hue"]),
    "Titles with Hues": (q17_titles_with_hues, ["Title","Hue"]),
    "Titles, Cultures & Ranks": (q18_titles_cultures_ranks, ["Title","Culture","Rank"]),
    "Top 10 Grey Hued Artifacts": (q19_top10_grey, ["Title"]),
    "Artifacts per Classification": (q20_artifacts_per_classification, ["Classification","Total","AvgMediaCount"])
}

choice = st.selectbox("Select a query:", list(query_map.keys()))
if st.button("Run Query"):
    func, cols = query_map[choice]
    result = func()
    if result:
        df = pd.DataFrame(result, columns=cols)
        st.dataframe(df)
    else:
        st.info("ℹNo results found.")

# --- User-Defined Queries ---
st.subheader("Run Your Own SQL Query")
user_sql = st.text_area("Enter your SQL query here:")
if st.button("Execute Custom Query"):
    if user_sql.strip():
        try:
            conn = get_conn()
            df = pd.read_sql(user_sql, conn)
            conn.close()
            if not df.empty:
                st.dataframe(df)
            else:
                st.info("Query executed successfully, but no rows found.")
        except Exception as e:
            st.error(f"Error: {e}")
    else:
        st.warning("Enter a SQL query before executing.")