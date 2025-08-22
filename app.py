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

# --- KONFIGURASI APLIKASI ---
st.set_page_config(page_title="Dashboard Pengolahan Data", layout="wide")

# --- KONFIGURASI PENGGUNA ---
# Ganti dengan ID Google Sheet Anda yang sebenarnya
SHEET_ID_DATA_LOOKER = "1RhHw8F9PN8c0_C3lBflrkBBO89BuFe5hvYRPJd8vH5c"
SHEET_ID_TIDAK_ADA_BRAND = "1Tu7hUiV7ZRijKLQWxWOVmv81ussqoPfKlkM5WFiHof0"

# Daftar file toko yang akan diproses
# Format: (Nama Toko, File Ready, File Habis)
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
    """Menghubungkan ke Google Sheets menggunakan kredensial dari Streamlit Secrets."""
    try:
        creds_dict = st.secrets["gcp_service_account"]
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"Gagal terhubung ke Google Sheets. Pastikan file 'secrets.toml' sudah benar. Detail: {e}")
        return None

@st.cache_data
def load_mapping_data():
    """Memuat dan memproses semua data mapping (database, brand, kamus) sekali saja untuk efisiensi."""
    try:
        # Membaca file-file mapping
        db_df = pd.read_csv("DATA_REKAP.xlsx - DATABASE.csv")
        brands_df = pd.read_csv("DATA_REKAP.xlsx - DATABASE_BRAND.csv", header=None)
        kamus_df = pd.read_csv("DATA_REKAP.xlsx - kamus_brand.csv")

        # 1. Mapping Produk DB KLIK (nama_lower -> (Brand, Kategori)) - Paling prioritas
        db_df_cleaned = db_df.dropna(subset=['NAMA', 'Brand', 'Kategori']).copy()
        db_df_cleaned['NAMA_lower'] = db_df_cleaned['NAMA'].str.lower()
        db_product_map = db_df_cleaned.set_index('NAMA_lower')[['Brand', 'Kategori']].apply(tuple, axis=1).to_dict()

        # 2. Mapping Kategori paling umum per Brand (untuk fuzzy)
        db_df_brand_cat = db_df.dropna(subset=['Brand', 'Kategori']).copy()
        most_common_category = db_df_brand_cat.groupby('Brand')['Kategori'].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None).to_dict()

        # 3. Buat Pola Regex Cepat untuk pencarian brand dan alias
        main_brands_lower = set(brands_df[0].str.lower().dropna())
        alias_map = {str(k).lower(): str(v) for k, v in kamus_df.set_index('Alias')['Brand_Utama'].to_dict().items()}
        
        # Mapping dari lowercase kembali ke nama brand original (case sensitive)
        cased_brands_map = {b.lower(): b for b in brands_df[0].dropna()}
        cased_brands_map.update(alias_map) # Alias sudah menunjuk ke brand utama

        all_search_terms = list(main_brands_lower) + list(alias_map.keys())
        sorted_terms = sorted(list(set(all_search_terms)), key=len, reverse=True)
        pattern = r'\b(' + '|'.join(re.escape(term) for term in sorted_terms) + r')\b'
        
        return db_product_map, most_common_category, pattern, alias_map, cased_brands_map
    except FileNotFoundError as e:
        st.error(f"FATAL: File mapping '{e.filename}' tidak ditemukan. Pastikan semua file CSV ada di folder yang sama.")
        return None, None, None, None, None

def find_brand_and_category(row, db_map, category_map, pattern, cased_map):
    """Fungsi cerdas untuk mencari brand dan kategori untuk satu baris data."""
    product_name_lower = str(row['NAMA']).lower()
    
    # Prioritas 1: Cek di database utama jika toko adalah DB KLIK
    if row['Toko'] == 'DB KLIK' and product_name_lower in db_map:
        return db_map[product_name_lower]

    # Prioritas 2: Cari menggunakan Regex dari daftar brand dan kamus
    match = re.search(pattern, product_name_lower)
    if match:
        term = match.group(1).lower()
        main_brand_name = cased_map.get(term) # Dapatkan nama brand utama yang sudah distandarisasi
        if main_brand_name:
            found_category = category_map.get(main_brand_name) # Ambil kategori paling umum
            return main_brand_name, found_category

    return None, None

def process_all_data(progress_bar):
    """Memproses semua file toko, menggabungkan, dan melabeli ulang."""
    all_dfs = []
    total_files = len(TOKO_FILES) * 2
    for i, (toko, ready_file, habis_file) in enumerate(TOKO_FILES):
        try:
            # Membaca dan memberi label pada setiap file
            df_ready = pd.read_csv(ready_file)
            df_ready['Toko'] = toko
            df_ready['Status'] = 'Ready'
            all_dfs.append(df_ready)
            progress_bar.progress((i * 2 + 1) / total_files, text=f"Membaca file: {ready_file}")

            df_habis = pd.read_csv(habis_file)
            df_habis['Toko'] = toko
            df_habis['Status'] = 'Habis'
            all_dfs.append(df_habis)
            progress_bar.progress((i * 2 + 2) / total_files, text=f"Membaca file: {habis_file}")

        except FileNotFoundError as e:
            st.warning(f"File tidak ditemukan: {e.filename}, akan dilewati.")
        except Exception as e:
            st.error(f"Error saat memproses file untuk toko {toko}: {e}")

    if not all_dfs:
        st.error("Tidak ada data yang berhasil dimuat. Proses dihentikan.")
        return None, None

    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    db_map, category_map, pattern, _, cased_map = load_mapping_data()
    if pattern is None: return None, None

    st.info("Memulai pelabelan ulang Brand dan Kategori...")
    results = combined_df.apply(lambda row: find_brand_and_category(row, db_map, category_map, pattern, cased_map), axis=1)
    combined_df[['BRAND_FINAL', 'KATEGORI_FINAL']] = pd.DataFrame(results.tolist(), index=combined_df.index)

    # Memisahkan data yang tidak teridentifikasi
    no_brand_df = combined_df[combined_df['BRAND_FINAL'].isnull()].copy()
    no_brand_final_df = no_brand_df[['TANGGAL', 'NAMA', 'Toko', 'Status']]
    
    return combined_df, no_brand_final_df

def link_similar_products(df, threshold=0.85):
    """Fungsi fuzzy matching untuk memberikan ID unik. TIDAK MENGUBAH KOLOM 'NAMA' ASLI."""
    st.info(f"Memulai Fuzzy Matching untuk {len(df):,} produk...")
    
    # 1. Membuat kolom SEMENTARA 'NAMA_CLEAN' untuk analisis. Kolom 'NAMA' asli aman.
    stop_words = ['original', 'garansi', 'resmi', 'murah', 'promo', 'bonus', 'free', 'laptop', 'mouse', 'keyboard', 'gaming', 'headset']
    def clean_text(text):
        text = str(text).lower()
        text = re.sub(r'[^\w\s]', ' ', text) # Hapus punctuation
        text = ' '.join([word for word in text.split() if word not in stop_words and not word.isdigit()])
        return text
    df['NAMA_CLEAN'] = df['NAMA'].apply(clean_text)
    
    df['PRODUCT_ID'] = -1
    unique_brands = df['BRAND_FINAL'].dropna().unique()
    progress_bar = st.progress(0, text="Memulai proses penautan produk...")
    
    # 2. Iterasi per brand (Blocking) untuk efisiensi
    for i, brand in enumerate(unique_brands):
        progress_bar.progress((i + 1) / len(unique_brands), text=f"Menganalisis brand: {brand}")
        
        brand_indices = df[df['BRAND_FINAL'] == brand].index
        if len(brand_indices) <= 1: continue

        names = df.loc[brand_indices, 'NAMA_CLEAN']
        
        # 3. TF-IDF & Cosine Similarity pada 'NAMA_CLEAN'
        vectorizer = TfidfVectorizer(min_df=1, analyzer='char_wb', ngram_range=(2, 4))
        tfidf_matrix = vectorizer.fit_transform(names)
        cosine_sim = cosine_similarity(tfidf_matrix)
        
        # 4. Clustering & Pemberian ID Unik
        visited = set()
        for j in range(len(brand_indices)):
            if j in visited: continue
            similar_items_mask = cosine_sim[j, :] >= threshold
            original_indices = [brand_indices[k] for k in np.where(similar_items_mask)[0]]
            group_id = original_indices[0] # Ambil index baris pertama sebagai ID grup
            df.loc[original_indices, 'PRODUCT_ID'] = group_id
            visited.update(np.where(similar_items_mask)[0])

    progress_bar.progress(1.0, "Proses penautan selesai!")
    return df

# --- TAMPILAN STREAMLIT ---
st.title("🚀 Aplikasi Pengolahan Data Penjualan untuk Looker Studio")
st.write("""
Aplikasi ini dirancang untuk membaca file-file CSV penjualan, melakukan standarisasi **Brand** & **Kategori**, dan secara opsional melakukan **Fuzzy Matching** untuk menemukan produk yang sama antar toko.
Pastikan semua file CSV (termasuk `DATABASE.csv`, `DATABASE_BRAND.csv`, `kamus_brand.csv`) berada dalam satu folder dengan aplikasi ini.
""")

do_fuzzy_matching = st.checkbox("✅ Lakukan Fuzzy Matching untuk Menghubungkan Produk (Membuat PRODUCT_ID)", value=True)
similarity_threshold = st.slider("Tingkat Kemiripan Produk (Threshold)", 0.7, 1.0, 0.85, 0.01, help="Semakin tinggi nilainya, produk harus semakin mirip untuk dianggap sama.")

if st.button("PROSES SEMUA DATA SEKARANG", type="primary"):
    start_time = time.time()
    
    gspread_client = connect_to_gsheets()
    if gspread_client:
        st.success("Berhasil terhubung dengan Google Sheets!")

        progress_bar_load = st.progress(0, text="Memulai proses pemuatan data...")
        processed_df, no_brand_df = process_all_data(progress_bar_load)

        if processed_df is not None:
            progress_bar_load.progress(1.0, text="Proses pelabelan selesai!")

            if do_fuzzy_matching:
                processed_df = link_similar_products(processed_df, threshold=similarity_threshold)
            
            try:
                # Menyiapkan data final untuk diunggah
                # Kolom sementara 'NAMA_CLEAN' dihapus di sini
                df_to_save = processed_df.drop(columns=['NAMA_CLEAN'], errors='ignore')

                with st.spinner(f"Menulis {len(df_to_save):,} baris data ke Google Sheet 'DATA_LOOKER'..."):
                    sh_looker = gspread_client.open_by_key(SHEET_ID_DATA_LOOKER)
                    worksheet_looker = sh_looker.sheet1
                    worksheet_looker.clear()
                    set_with_dataframe(worksheet_looker, df_to_save)
                st.success("Data utama berhasil ditulis ke 'DATA_LOOKER'!")

                with st.spinner(f"Menulis {len(no_brand_df):,} baris data ke 'TIDAK_ADA_BRAND'..."):
                    sh_nobrand = gspread_client.open_by_key(SHEET_ID_TIDAK_ADA_BRAND)
                    worksheet_nobrand = sh_nobrand.sheet1
                    worksheet_nobrand.clear()
                    set_with_dataframe(worksheet_nobrand, no_brand_df)
                st.success("Data tanpa brand berhasil ditulis ke 'TIDAK_ADA_BRAND'!")
                
                end_time = time.time()
                st.info(f"Total waktu pemrosesan: {end_time - start_time:.2f} detik.")
                st.balloons()

                st.header("Hasil Pemrosesan")
                st.metric("Total Baris Data Diproses", f"{len(df_to_save):,}")
                st.metric("Jumlah Produk Tanpa Brand Ditemukan", f"{len(no_brand_df):,}")
                
                st.subheader("Contoh Data yang Berhasil Diproses (Kolom 'NAMA' Tetap Asli)")
                st.dataframe(df_to_save.head())

            except Exception as e:
                st.error(f"Gagal saat menulis ke Google Sheets: {e}")
