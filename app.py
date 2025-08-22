import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import re
import time
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

# --- KONFIGURASI ---
st.set_page_config(page_title="Data Processing Dashboard", layout="wide")

# Konfigurasi file dan Google Sheets
SHEET_ID_DATA_LOOKER = "1RhHw8F9PN8c0_C3lBflrkBBO89BuFe5hvYRPJd8vH5c"
SHEET_ID_TIDAK_ADA_BRAND = "1Tu7hUiV7ZRijKLQWxWOVmv81ussqoPfKlkM5WFiHof0"

TOKO_FILES = [
    ("DB KLIK", "DATA_REKAP.xlsx - DB KLIK - REKAP - READY.csv", "DATA_REKAP.xlsx - DB KLIK - REKAP - HABIS.csv"),
    ("ABDITAMA", "DATA_REKAP.xlsx - ABDITAMA - REKAP - READY.csv", "DATA_REKAP.xlsx - ABDITAMA - REKAP - HABIS.csv"),
    ("LEVEL99", "DATA_REKAP.xlsx - LEVEL99 - REKAP - READY.csv", "DATA_REKAP.xlsx - LEVEL99 - REKAP - HABIS.csv"),
    ("IT SHOP", "DATA_REKAP.xlsx - IT SHOP - REKAP - READY.csv", "DATA_REKAP.xlsx - IT SHOP - REKAP - HABIS.csv"),
    ("JAYA PC", "DATA_REKAP.xlsx - JAYA PC - REKAP - READY.csv", "DATA_REKAP.xlsx - JAYA PC - REKAP - HABIS.csv"),
    ("MULTIFUNGSI", "DATA_REKAP.xlsx - MULTIFUNGSI - REKAP - READY.csv", "DATA_REKAP.xlsx - MULTIFUNGSI - REKAP - HABIS.csv"),
    ("TECH ISLAND", "DATA_REKAP.xlsx - TECH ISLAND - REKAP - READY.csv", "DATA_REKAP.xlsx - TECH ISLAND - REKAP - HABIS.csv"),
    ("GG STORE", "DATA_REKAP.xlsx - GG STORE - REKAP - READY.csv", "DATA_REKAP.xlsx - GG STORE - REKAP - HABIS.csv"),
    ("SURYA MITRA ONLINE", "DATA_REKAP.xlsx - SURYA MITRA ONLINE - REKAP - RE.csv", "DATA_REKAP.xlsx - SURYA MITRA ONLINE - REKAP - HA.csv"),
]

# --- FUNGSI-FUNGSI UTAMA ---

@st.cache_resource
def connect_to_gsheets():
    creds_dict = st.secrets["gcp_service_account"]
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client

@st.cache_data
def load_mapping_data():
    try:
        db_df = pd.read_csv("DATA_REKAP.xlsx - DATABASE.csv")
        brands_df = pd.read_csv("DATA_REKAP.xlsx - DATABASE_BRAND.csv", header=None)
        kamus_df = pd.read_csv("DATA_REKAP.xlsx - kamus_brand.csv")
        db_product_map = {
            row['NAMA'].lower(): (row['Brand'], row['Kategori'])
            for _, row in db_df.dropna(subset=['NAMA', 'Brand', 'Kategori']).iterrows()
        }
        most_common_category = db_df.dropna(subset=['Brand', 'Kategori']).groupby('Brand')['Kategori'].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None).to_dict()
        main_brands_lower = set(brands_df[0].str.lower().dropna())
        alias_map = kamus_df.dropna().set_index(kamus_df.iloc[:, 0].str.lower())[kamus_df.columns[1]].to_dict()
        cased_brands_map = {b.lower(): b for b in brands_df[0].dropna()}
        cased_brands_map.update({a.lower(): b for a, b in kamus_df.dropna().values})
        all_search_terms = list(main_brands_lower) + list(alias_map.keys())
        sorted_terms = sorted(list(set(all_search_terms)), key=len, reverse=True)
        pattern = r'\b(' + '|'.join(re.escape(term) for term in sorted_terms) + r')\b'
        return db_product_map, most_common_category, pattern, alias_map, cased_brands_map
    except FileNotFoundError as e:
        st.error(f"Error: File mapping tidak ditemukan! Pastikan file {e.filename} ada di folder yang sama.")
        return None, None, None, None, None

def find_brand_and_category(row, db_map, category_map, pattern, alias_map, cased_map):
    product_name_lower = str(row['NAMA']).lower()
    toko = row['Toko']
    if toko == 'DB KLIK' and product_name_lower in db_map:
        return db_map[product_name_lower]
    match = re.search(pattern, product_name_lower)
    if match:
        term = match.group(1)
        main_brand_name = cased_map.get(term)
        if main_brand_name:
            found_category = category_map.get(main_brand_name)
            return main_brand_name, found_category
    return None, None

def process_all_data(progress_bar):
    all_dfs = []
    total_files = len(TOKO_FILES) * 2
    for i, (toko, ready_file, habis_file) in enumerate(TOKO_FILES):
        try:
            df_ready = pd.read_csv(ready_file)
            df_ready['Toko'] = toko
            df_ready['Status'] = 'Ready'
            all_dfs.append(df_ready)
            progress_bar.progress((i * 2 + 1) / total_files, text=f"Memuat file: {ready_file}")
            df_habis = pd.read_csv(habis_file)
            df_habis['Toko'] = toko
            df_habis['Status'] = 'Habis'
            all_dfs.append(df_habis)
            progress_bar.progress((i * 2 + 2) / total_files, text=f"Memuat file: {habis_file}")
        except FileNotFoundError as e:
            st.warning(f"File tidak ditemukan: {e.filename}. Melewati file ini.")
    if not all_dfs: return None, None
    combined_df = pd.concat(all_dfs, ignore_index=True)
    db_map, category_map, pattern, alias_map, cased_map = load_mapping_data()
    if pattern is None: return None, None
    st.info("Memulai proses pelabelan ulang Brand dan Kategori...")
    results = combined_df.apply(lambda row: find_brand_and_category(row, db_map, category_map, pattern, alias_map, cased_map), axis=1)
    combined_df[['BRAND_FINAL', 'KATEGORI_FINAL']] = pd.DataFrame(results.tolist(), index=combined_df.index)
    no_brand_df = combined_df[combined_df['BRAND_FINAL'].isnull()]
    no_brand_final_df = no_brand_df[['TANGGAL', 'NAMA', 'Toko', 'Status']].copy()
    no_brand_final_df.rename(columns={'Toko': 'TOKO', 'Status': 'STATUS'}, inplace=True)
    return combined_df, no_brand_final_df

def link_similar_products(df, threshold=0.85):
    """Fungsi untuk melakukan fuzzy matching dan memberikan ID unik pada produk yang sama."""
    st.info(f"Memulai Fuzzy Matching untuk {len(df):,} produk...")
    
    # 1. Preprocessing
    stop_words = ['original', 'garansi', 'resmi', 'murah', 'promo', 'bonus', 'free', 'laptop', 'mouse', 'keyboard', 'headset']
    def clean_text(text):
        text = str(text).lower()
        text = re.sub(r'[^\w\s]', '', text) # Hapus Punctuation
        text = ' '.join([word for word in text.split() if word not in stop_words])
        return text

    df['NAMA_CLEAN'] = df['NAMA'].apply(clean_text)
    
    # Inisialisasi kolom PRODUCT_ID
    df['PRODUCT_ID'] = -1
    
    unique_brands = df['BRAND_FINAL'].dropna().unique()
    
    progress_bar = st.progress(0, text="Memulai proses linking produk...")
    
    # 2. Blocking & Matching
    for i, brand in enumerate(unique_brands):
        progress_bar.progress(i / len(unique_brands), text=f"Menganalisis brand: {brand}")
        
        brand_indices = df[df['BRAND_FINAL'] == brand].index
        
        # Lewati jika hanya ada 1 produk, langsung beri ID
        if len(brand_indices) <= 1:
            if len(brand_indices) == 1:
                df.loc[brand_indices, 'PRODUCT_ID'] = brand_indices[0]
            continue

        # Ambil nama produk yang sudah bersih untuk brand ini
        names = df.loc[brand_indices, 'NAMA_CLEAN']
        
        # 3. TF-IDF & Cosine Similarity
        vectorizer = TfidfVectorizer(min_df=1, analyzer='char_wb', ngram_range=(2, 4))
        tfidf_matrix = vectorizer.fit_transform(names)
        cosine_sim = cosine_similarity(tfidf_matrix)
        
        # 4. Clustering & Pemberian ID Unik
        visited = set()
        for j in range(len(brand_indices)):
            if j in visited:
                continue
            
            # Temukan semua produk yang mirip dengan produk j
            similar_items_mask = cosine_sim[j, :] >= threshold
            similar_items_indices_in_block = np.where(similar_items_mask)[0]
            
            # Dapatkan index asli dari DataFrame
            original_indices = [brand_indices[k] for k in similar_items_indices_in_block]
            
            # Gunakan index dari item pertama sebagai ID unik untuk grup ini
            group_id = original_indices[0]
            df.loc[original_indices, 'PRODUCT_ID'] = group_id
            
            visited.update(similar_items_indices_in_block)

    progress_bar.progress(1.0, "Proses linking selesai!")
    return df

# --- TAMPILAN STREAMLIT ---
st.title("🚀 Aplikasi Pengolahan dan Penautan Data Penjualan")
st.write("""
Aplikasi ini akan memproses data penjualan, melakukan standarisasi **Brand** & **Kategori**, dan secara opsional melakukan **Fuzzy Matching** untuk menemukan produk yang sama antar toko dan memberikan `PRODUCT_ID` yang unik.
""")

do_fuzzy_matching = st.checkbox("✅ Lakukan Fuzzy Matching untuk Menghubungkan Produk Antar Toko (Proses lebih lama)")

if st.button("Proses Semua Data Sekarang", type="primary"):
    start_time = time.time()
    
    with st.spinner("Menghubungkan ke Google Sheets..."):
        gspread_client = connect_to_gsheets()
    st.success("Berhasil terhubung dengan Google Sheets!")

    progress_bar_load = st.progress(0, text="Memulai proses pemuatan data...")
    processed_df, no_brand_df = process_all_data(progress_bar_load)

    if processed_df is not None:
        progress_bar_load.progress(1.0, text="Proses pelabelan selesai!")

        # Jalankan Fuzzy Matching jika dicentang
        if do_fuzzy_matching:
            processed_df = link_similar_products(processed_df)
        
        try:
            with st.spinner(f"Menulis {len(processed_df)} baris data ke Google Sheet 'DATA_LOOKER'..."):
                sh_looker = gspread_client.open_by_key(SHEET_ID_DATA_LOOKER)
                worksheet_looker = sh_looker.sheet1
                worksheet_looker.clear()
                # Hapus kolom helper sebelum menyimpan
                df_to_save = processed_df.drop(columns=['NAMA_CLEAN'], errors='ignore')
                set_with_dataframe(worksheet_looker, df_to_save)
            st.success("Data utama berhasil ditulis ke 'DATA_LOOKER'!")

            with st.spinner(f"Menulis {len(no_brand_df)} baris data ke Google Sheet 'TIDAK_ADA_BRAND'..."):
                sh_nobrand = gspread_client.open_by_key(SHEET_ID_TIDAK_ADA_BRAND)
                worksheet_nobrand = sh_nobrand.sheet1
                worksheet_nobrand.clear()
                set_with_dataframe(worksheet_nobrand, no_brand_df)
            st.success("Data tanpa brand berhasil ditulis ke 'TIDAK_ADA_BRAND'!")
            
            end_time = time.time()
            st.info(f"Total waktu pemrosesan: {end_time - start_time:.2f} detik.")
            st.balloons()

            st.header("Hasil Pemrosesan")
            st.metric("Total Baris Data Diproses", f"{len(processed_df):,}")
            st.metric("Jumlah Produk Tanpa Brand", f"{len(no_brand_df):,}")
            
            st.subheader("Contoh Data yang Berhasil Diproses")
            st.dataframe(processed_df.head())

        except Exception as e:
            st.error(f"Terjadi kesalahan: {e}")
