# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR
#  Versi: Logika Penulisan Terpisah
# ===================================================================================

# ===================================================================================
# IMPORT LIBRARY
# ===================================================================================
import streamlit as st
import pandas as pd
from thefuzz import process, fuzz
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import warnings
import re

warnings.filterwarnings('ignore', category=UserWarning, module='gspread_dataframe')

# ===================================================================================
# KONFIGURASI HALAMAN & URL
# ===================================================================================
st.set_page_config(layout="wide", page_title="Automasi Pelabelan")

# URL Google Sheets Anda
SOURCE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ"
DESTINATION_SHEET_URL = "https://docs.google.com/spreadsheets/d/1RhHw8F9PN8c0_C3lBflrkBBO89BuFe5hvYRPJd8vH5c"
MISSING_INFO_SHEET_URL = "https://docs.google.com/spreadsheets/d/1Tu7hUiV7ZRijKLQWxWOVmv81ussqoPfKlkM5WFiHof0"

# ===================================================================================
# FUNGSI-FUNGSI UTAMA
# ===================================================================================

@st.cache_resource
def get_gspread_client():
    """Mengautentikasi ke Google API menggunakan Streamlit Secrets."""
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

def check_brand_and_category(product_name, database_df, all_brands_list):
    """
    Memeriksa brand dan kategori produk dengan metode 3-langkah:
    1. Direct Match: Mencari kecocokan nama produk 100%.
    2. Fuzzy Match: Mencari nama produk yang sangat mirip (skor > 90).
    3. Keyword Search: Mencari kata kunci brand di dalam nama produk.
    """
    if not isinstance(product_name, str) or not product_name.strip():
        return None, None

    product_name_lower = product_name.lower().strip()

    # --- Langkah 1: Direct Match ---
    direct_match = database_df[database_df['NAMA'].str.lower() == product_name_lower]
    if not direct_match.empty:
        return direct_match.iloc[0]['Brand'], direct_match.iloc[0]['Kategori']

    # --- Langkah 2: Fuzzy Match ---
    match, score = process.extractOne(product_name_lower, database_df['NAMA'].str.lower(), scorer=fuzz.token_sort_ratio)
    if score >= 90:
        matched_row = database_df[database_df['NAMA'].str.lower() == match]
        if not matched_row.empty:
            return matched_row.iloc[0]['Brand'], matched_row.iloc[0]['Kategori']

    # --- Langkah 3: Keyword Search ---
    for brand in all_brands_list:
        if re.search(r'\b' + re.escape(brand.lower()) + r'\b', product_name_lower):
            return brand.upper(), None # Kategori sengaja dikosongkan agar masuk daftar cek manual

    return None, None

@st.cache_data(show_spinner="Memproses dan melabeli data...")
def process_and_label_data(_client):
    """Fungsi utama untuk memuat, menggabungkan, dan memproses data."""
    source_spreadsheet = _client.open_by_url(SOURCE_SHEET_URL)
    
    db_df = pd.DataFrame(source_spreadsheet.worksheet("DATABASE").get_all_records())
    db_brand_df = pd.DataFrame(source_spreadsheet.worksheet("DATABASE_BRAND").get_all_records())
    
    db_df.dropna(subset=['NAMA', 'Brand', 'Kategori'], inplace=True)
    all_brands_list = db_brand_df.iloc[:, 0].dropna().unique().tolist()
    
    all_sheets = source_spreadsheet.worksheets()
    exclude_sheets = ["DATABASE", "DATABASE_BRAND", "kamus_brand", "DB KLIK - REKAP - READY", "DB KLIK - REKAP - HABIS"]
    df_list = []

    for sheet in all_sheets:
        if sheet.title not in exclude_sheets:
            data = sheet.get_all_records()
            if not data: continue
            
            df = pd.DataFrame(data)
            parts = sheet.title.split(' - REKAP - ')
            df['Toko'] = parts[0].strip() if len(parts) == 2 else sheet.title
            df['Status'] = parts[1].strip() if len(parts) == 2 else 'Unknown'
            df_list.append(df)
            
    if not df_list:
        return pd.DataFrame(), pd.DataFrame()

    combined_df = pd.concat(df_list, ignore_index=True)

    # Terapkan fungsi pengecekan
    results = combined_df.apply(
        lambda row: check_brand_and_category(row['NAMA'], db_df, all_brands_list),
        axis=1,
        result_type='expand'
    )
    combined_df[['BRAND_HASIL', 'KATEGORI_HASIL']] = results

    # ---- PERUBAHAN LOGIKA DI SINI ----
    # 1. DataFrame utama adalah SEMUA data
    all_data_final = combined_df.copy()

    # 2. DataFrame data kurang adalah HANYA data yang brand atau kategorinya kosong
    missing_data = all_data_final[all_data_final['BRAND_HASIL'].isna() | all_data_final['KATEGORI_HASIL'].isna()].copy()

    return all_data_final, missing_data

def write_to_gsheet(client, sheet_url, worksheet_name, df_to_write):
    """Menulis DataFrame ke worksheet yang ditentukan."""
    try:
        spreadsheet = client.open_by_url(sheet_url)
        worksheet = spreadsheet.worksheet(worksheet_name)
        worksheet.clear()
        set_with_dataframe(worksheet, df_to_write, resize=True)
        st.success(f"Berhasil menulis {len(df_to_write)} baris ke sheet '{worksheet_name}'.")
    except gspread.exceptions.WorksheetNotFound:
        st.warning(f"Worksheet '{worksheet_name}' tidak ditemukan. Membuat sheet baru...")
        spreadsheet.add_worksheet(title=worksheet_name, rows=len(df_to_write)+1, cols=len(df_to_write.columns))
        worksheet = spreadsheet.worksheet(worksheet_name)
        set_with_dataframe(worksheet, df_to_write, resize=True)
        st.success(f"Berhasil menulis ke sheet '{worksheet_name}' yang baru dibuat.")
    except Exception as e:
        st.error(f"Gagal menulis data ke sheet '{worksheet_name}': {e}")

# ===================================================================================
# TAMPILAN APLIKASI STREAMLIT
# ===================================================================================
st.title("🚀 Automasi Pelabelan Brand & Kategori")

st.info("""
Aplikasi ini akan memproses semua data produk Anda.
- **Spreadsheet Utama:** Berisi SEMUA produk. Produk yang tidak teridentifikasi akan memiliki kolom `BRAND_HASIL` dan `KATEGORI_HASIL` yang kosong.
- **Spreadsheet Data Kurang:** Berisi DAFTAR produk yang perlu Anda periksa dan lengkapi secara manual.
""")

if st.button("Mulai Proses Pelabelan", type="primary"):
    with st.spinner("Menjalankan proses... Ini mungkin memakan waktu beberapa saat."):
        client = get_gspread_client()
        
        st.header("1. Memproses Data")
        all_processed_data, missing_info_df = process_and_label_data(client)
        
        st.success(f"Pemrosesan selesai. Total {len(all_processed_data)} baris data diproses.")
        st.warning(f"Ditemukan {len(missing_info_df)} produk yang memerlukan pemeriksaan manual.")

        st.header("2. Menulis Hasil ke Google Sheets")

        # Menulis SEMUA data ke spreadsheet utama
        if not all_processed_data.empty:
            cols_to_keep_main = ['TANGGAL', 'NAMA', 'HARGA', 'TERJUAL/BLN', 'BRAND', 'Toko', 'Status', 'BRAND_HASIL', 'KATEGORI_HASIL']
            final_df_main = all_processed_data[[col for col in cols_to_keep_main if col in all_processed_data.columns]]
            write_to_gsheet(client, DESTINATION_SHEET_URL, "Hasil Proses Lengkap", final_df_main)
            st.subheader("Contoh Data yang Ditulis ke Spreadsheet Utama")
            st.dataframe(final_df_main.head())
        
        # Menulis data yang KURANG ke spreadsheet terpisah
        if not missing_info_df.empty:
            cols_to_keep_missing = ['TANGGAL', 'NAMA', 'Toko', 'Status']
            final_missing_df = missing_info_df[[col for col in cols_to_keep_missing if col in missing_info_df.columns]]
            write_to_gsheet(client, MISSING_INFO_SHEET_URL, "Perlu Dicek Manual", final_missing_df)
            st.subheader("Contoh Data yang Perlu Dicek Manual")
            st.dataframe(final_missing_df.head())
        else:
            st.balloons()
            st.success("Luar biasa! Semua data berhasil diidentifikasi dengan lengkap.")
