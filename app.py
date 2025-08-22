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
# ID untuk Google Sheet SUMBER, TUJUAN DATA, dan TUJUAN DATA TANPA BRAND
SHEET_ID_DATA_REKAP = "1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ"
SHEET_ID_DATA_LOOKER = "1RhHw8F9PN8c0_C3lBflrkBBO89BuFe5hvYRPJd8vH5c"
SHEET_ID_TIDAK_ADA_BRAND = "1Tu7hUiV7ZRijKLQWxWOVmv81ussqoPfKlkM5WFiHof0"

# Daftar NAMA SHEET (TAB) di dalam Google Sheet DATA_REKAP yang akan diproses
# Pastikan nama-nama ini sama persis dengan yang ada di spreadsheet Anda
STORE_SHEET_NAMES = [
    'DB KLIK - REKAP - READY', 'DB KLIK - REKAP - HABIS',
    'ABDITAMA - REKAP - READY', 'ABDITAMA - REKAP - HABIS',
    'LEVEL99 - REKAP - READY', 'LEVEL99 - REKAP - HABIS',
    'IT SHOP - REKAP - READY', 'IT SHOP - REKAP - HABIS',
    'JAYA PC - REKAP - READY', 'JAYA PC - REKAP - HABIS',
    'MULTIFUNGSI - REKAP - READY', 'MULTIFUNGSI - REKAP - HABIS',
    'TECH ISLAND - REKAP - READY', 'TECH ISLAND - REKAP - HABIS',
    'GG STORE - REKAP - READY', 'GG STORE - REKAP - HABIS',
    'SURYA MITRA ONLINE - REKAP - READY', 'SURYA MITRA ONLINE - REKAP - HABIS'
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
def load_mapping_data(_spreadsheet_obj):
    """Memuat data mapping (DATABASE, DATABASE_BRAND, kamus_brand) dari Google Sheet sumber."""
    st.info("Membaca sheet 'DATABASE', 'DATABASE_BRAND', dan 'kamus_brand'...")
    try:
        # Membaca data dari masing-masing sheet
        db_sheet = _spreadsheet_obj.worksheet('DATABASE')
        db_df = pd.DataFrame(db_sheet.get_all_records())

        brand_sheet = _spreadsheet_obj.worksheet('DATABASE_BRAND')
        # Asumsi brand hanya di kolom pertama
        brands_df = pd.DataFrame(brand_sheet.get_all_values())[0]

        kamus_sheet = _spreadsheet_obj.worksheet('kamus_brand')
        kamus_df = pd.DataFrame(kamus_sheet.get_all_records())

        # Proses data mapping (sama seperti sebelumnya, tapi sumbernya dari GSheet)
        db_product_map = {str(row['NAMA']).lower(): (row['Brand'], row['Kategori']) for _, row in db_df.dropna(subset=['NAMA', 'Brand', 'Kategori']).iterrows()}
        most_common_category = db_df.dropna(subset=['Brand', 'Kategori']).groupby('Brand')['Kategori'].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None).to_dict()
        main_brands_lower = set(brands_df.str.lower().dropna())
        alias_map = {str(k).lower(): str(v) for k, v in kamus_df.set_index('Alias')['Brand_Utama'].to_dict().items()}
        cased_brands_map = {b.lower(): b for b in brands_df.dropna()}
        cased_brands_map.update(alias_map)
        all_search_terms = list(main_brands_lower) + list(alias_map.keys())
        sorted_terms = sorted(list(set(all_search_terms)), key=len, reverse=True)
        pattern = r'\b(' + '|'.join(re.escape(term) for term in sorted_terms) + r')\b'
        
        return db_product_map, most_common_category, pattern, cased_brands_map
    except gspread.exceptions.WorksheetNotFound as e:
        st.error(f"FATAL: Sheet mapping '{e}' tidak ditemukan di Google Sheet DATA_REKAP.")
        return None, None, None, None
    except Exception as e:
        st.error(f"Terjadi error saat memuat data mapping: {e}")
        return None, None, None, None

def find_brand_and_category(row, db_map, category_map, pattern, cased_map):
    """Fungsi cerdas untuk mencari brand dan kategori untuk satu baris data."""
    product_name_lower = str(row['NAMA']).lower()
    if row['Toko'] == 'DB KLIK' and product_name_lower in db_map: return db_map[product_name_lower]
    match = re.search(pattern, product_name_lower)
    if match:
        term = match.group(1).lower()
        main_brand_name = cased_map.get(term)
        if main_brand_name:
            found_category = category_map.get(main_brand_name)
            return main_brand_name, found_category
    return None, None

def process_all_data(_spreadsheet_obj, progress_bar):
    """Membaca semua sheet toko dari Google Sheet, menggabungkan, dan melabeli ulang."""
    all_dfs = []
    db_map, category_map, pattern, cased_map = load_mapping_data(_spreadsheet_obj)
    if pattern is None: return None, None

    for i, sheet_name in enumerate(STORE_SHEET_NAMES):
        try:
            progress_bar.progress((i + 1) / len(STORE_SHEET_NAMES), text=f"Membaca sheet: {sheet_name}")
            worksheet = _spreadsheet_obj.worksheet(sheet_name)
            df = pd.DataFrame(worksheet.get_all_records())
            
            # Menentukan Nama Toko dan Status dari Nama Sheet
            parts = sheet_name.split(' - REKAP - ')
            df['Toko'] = parts[0]
            status_part = parts[1]
            if status_part == 'READY' or status_part == 'RE':
                df['Status'] = 'Ready'
            elif status_part == 'HABIS' or status_part == 'HA':
                df['Status'] = 'Habis'
            else:
                df['Status'] = 'Unknown'
            all_dfs.append(df)
        except gspread.exceptions.WorksheetNotFound:
            st.warning(f"Sheet '{sheet_name}' tidak ditemukan, akan dilewati.")
        except Exception as e:
            st.error(f"Error saat memproses sheet '{sheet_name}': {e}")
    
    if not all_dfs:
        st.error("Tidak ada data toko yang berhasil dimuat. Proses dihentikan.")
        return None, None

    combined_df = pd.concat(all_dfs, ignore_index=True)
    st.info(f"Total {len(combined_df):,} baris data dari semua toko berhasil digabungkan.")

    st.info("Memulai pelabelan ulang Brand dan Kategori...")
    results = combined_df.apply(lambda row: find_brand_and_category(row, db_map, category_map, pattern, cased_map), axis=1)
    combined_df[['BRAND_FINAL', 'KATEGORI_FINAL']] = pd.DataFrame(results.tolist(), index=combined_df.index)

    no_brand_df = combined_df[combined_df['BRAND_FINAL'].isnull()].copy()
    no_brand_final_df = no_brand_df[['TANGGAL', 'NAMA', 'Toko', 'Status']]
    
    return combined_df, no_brand_final_df

def link_similar_products(df, threshold=0.85):
    """Fungsi fuzzy matching yang TIDAK MENGUBAH KOLOM 'NAMA' ASLI."""
    st.info(f"Memulai Fuzzy Matching untuk {len(df):,} produk...")
    stop_words = ['original', 'garansi', 'resmi', 'murah', 'promo', 'bonus', 'free', 'laptop', 'mouse', 'keyboard', 'gaming', 'headset']
    def clean_text(text):
        text = str(text).lower()
        text = re.sub(r'[^\w\s]', ' ', text)
        text = ' '.join([word for word in text.split() if word not in stop_words and not word.isdigit()])
        return text
    df['NAMA_CLEAN'] = df['NAMA'].apply(clean_text)
    
    df['PRODUCT_ID'] = -1
    unique_brands = df['BRAND_FINAL'].dropna().unique()
    progress_bar = st.progress(0, text="Memulai proses penautan produk...")
    
    for i, brand in enumerate(unique_brands):
        progress_bar.progress((i + 1) / len(unique_brands), text=f"Menganalisis brand: {brand}")
        brand_indices = df[df['BRAND_FINAL'] == brand].index
        if len(brand_indices) <= 1: continue
        names = df.loc[brand_indices, 'NAMA_CLEAN']
        vectorizer = TfidfVectorizer(min_df=1, analyzer='char_wb', ngram_range=(2, 4))
        tfidf_matrix = vectorizer.fit_transform(names)
        cosine_sim = cosine_similarity(tfidf_matrix)
        
        visited = set()
        for j in range(len(brand_indices)):
            if j in visited: continue
            similar_items_mask = cosine_sim[j, :] >= threshold
            original_indices = [brand_indices[k] for k in np.where(similar_items_mask)[0]]
            group_id = original_indices[0]
            df.loc[original_indices, 'PRODUCT_ID'] = group_id
            visited.update(np.where(similar_items_mask)[0])

    progress_bar.progress(1.0, "Proses penautan selesai!")
    return df

# --- TAMPILAN STREAMLIT ---
st.title("🚀 Aplikasi Pengolahan Data dari Google Sheet")
st.write("Aplikasi ini akan membaca semua data langsung dari setiap *sheet* (tab) di dalam Google Sheet **DATA_REKAP**, memprosesnya, dan mengirimkan hasilnya ke sheet tujuan.")

do_fuzzy_matching = st.checkbox("✅ Lakukan Fuzzy Matching untuk Menghubungkan Produk (Membuat PRODUCT_ID)", value=True)
similarity_threshold = st.slider("Tingkat Kemiripan Produk (Threshold)", 0.7, 1.0, 0.85, 0.01)

if st.button("PROSES SEMUA DATA DARI GOOGLE SHEET", type="primary"):
    start_time = time.time()
    gspread_client = connect_to_gsheets()
    if gspread_client:
        try:
            st.info(f"Membuka Google Sheet sumber (ID: ...{SHEET_ID_DATA_REKAP[-6:]})")
            source_spreadsheet = gspread_client.open_by_key(SHEET_ID_DATA_REKAP)
            
            progress_bar_load = st.progress(0, text="Memulai...")
            processed_df, no_brand_df = process_all_data(source_spreadsheet, progress_bar_load)

            if processed_df is not None:
                progress_bar_load.progress(1.0, "Proses pelabelan selesai!")

                if do_fuzzy_matching:
                    processed_df = link_similar_products(processed_df, threshold=similarity_threshold)
                
                df_to_save = processed_df.drop(columns=['NAMA_CLEAN'], errors='ignore')

                with st.spinner(f"Menulis {len(df_to_save):,} baris ke 'DATA_LOOKER'..."):
                    sh_looker = gspread_client.open_by_key(SHEET_ID_DATA_LOOKER)
                    worksheet_looker = sh_looker.sheet1
                    worksheet_looker.clear()
                    set_with_dataframe(worksheet_looker, df_to_save)
                st.success("Data utama berhasil ditulis ke 'DATA_LOOKER'!")

                with st.spinner(f"Menulis {len(no_brand_df):,} baris ke 'TIDAK_ADA_BRAND'..."):
                    sh_nobrand = gspread_client.open_by_key(SHEET_ID_TIDAK_ADA_BRAND)
                    worksheet_nobrand = sh_nobrand.sheet1
                    worksheet_nobrand.clear()
                    set_with_dataframe(worksheet_nobrand, no_brand_df)
                st.success("Data tanpa brand berhasil ditulis ke 'TIDAK_ADA_BRAND'!")
                
                end_time = time.time()
                st.info(f"Total waktu pemrosesan: {end_time - start_time:.2f} detik.")
                st.balloons()
                st.dataframe(df_to_save.head())

        except gspread.exceptions.SpreadsheetNotFound:
            st.error(f"Google Sheet dengan ID '{SHEET_ID_DATA_REKAP}' tidak ditemukan. Pastikan ID sudah benar dan service account memiliki akses.")
        except Exception as e:
            st.error(f"Terjadi kesalahan tak terduga: {e}")
