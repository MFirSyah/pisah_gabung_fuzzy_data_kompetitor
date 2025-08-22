import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import warnings
from thefuzz import process, fuzz
import re

warnings.filterwarnings('ignore', category=UserWarning, module='gspread_dataframe')

# --- Konfigurasi Google Sheets ---
SOURCE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ"
DESTINATION_SHEET_URL = "https://docs.google.com/spreadsheets/d/1RhHw8F9PN8c0_C3lBflrkBBO89BuFe5hvYRPJd8vH5c"
MISSING_INFO_SHEET_URL = "https://docs.google.com/spreadsheets/d/1Tu7hUiV7ZRijKLQWxWOVmv81ussqoPfKlkM5WFiHof0"

# --- Fungsi Autentikasi ke Google Sheets ---
@st.cache_resource
def get_gspread_client():
    """Mengautentikasi ke Google Sheets menggunakan st.secrets."""
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

# --- Fungsi untuk Memuat Data Referensi (Struktur Anda Dipertahankan) ---
@st.cache_data(show_spinner="Memuat data referensi dari Google Sheets...")
def load_reference_data(_client):
    """Memuat sheet referensi (DATABASE, kamus, dll.) ke dalam DataFrame."""
    try:
        source_spreadsheet = _client.open_by_url(SOURCE_SHEET_URL)
        db_df = pd.DataFrame(source_spreadsheet.worksheet("DATABASE").get_all_records())
        db_brand_df = pd.DataFrame(source_spreadsheet.worksheet("DATABASE_BRAND").get_all_records())
        
        # --- PERBAIKAN STABILITAS ---
        # Hanya hapus baris jika 'NAMA' benar-benar kosong
        db_df.dropna(subset=['NAMA'], inplace=True)
        if 'Brand' not in db_df.columns: db_df['Brand'] = None
        if 'Kategori' not in db_df.columns: db_df['Kategori'] = None
        
        # Mengambil daftar brand unik
        all_brands_list = []
        if not db_brand_df.empty:
            all_brands_list = db_brand_df.iloc[:, 0].dropna().unique().tolist()
            
        return db_df, all_brands_list, source_spreadsheet
    except Exception as e:
        st.error(f"Gagal memuat data referensi: {e}")
        return None, None, None

# --- FUNGSI DETEKSI CANGGIHAN YANG DICANGKOKKAN ---
def check_brand_and_category(product_name, database_df, all_brands_list):
    """Memeriksa brand & kategori dengan metode 3-langkah: Direct, Fuzzy, Keyword."""
    if not isinstance(product_name, str) or not product_name.strip():
        return None, None

    product_name_lower = product_name.lower().strip()

    # Langkah 1: Direct Match
    direct_match = database_df[database_df['NAMA'].str.lower() == product_name_lower]
    if not direct_match.empty:
        return direct_match.iloc[0]['Brand'], direct_match.iloc[0]['Kategori']

    # Langkah 2: Fuzzy Match (dengan pengaman)
    choices = database_df['NAMA'].dropna().str.lower()
    if not choices.empty:
        match, score = process.extractOne(product_name_lower, choices, scorer=fuzz.token_sort_ratio)
        if score >= 90:
            matched_row = database_df[database_df['NAMA'].str.lower() == match]
            if not matched_row.empty:
                return matched_row.iloc[0]['Brand'], matched_row.iloc[0]['Kategori']

    # Langkah 3: Keyword Search
    for brand in all_brands_list:
        if re.search(r'\b' + re.escape(brand.lower()) + r'\b', product_name_lower):
            return brand.upper(), None # Kategori kosong agar masuk daftar cek manual

    return None, None

# --- FUNGSI UTAMA UNTUK MEMPROSES DATA (SUDAH DIUPGRADE) ---
def process_data(source_spreadsheet, db_df, all_brands_list):
    """Menggabungkan data toko dan melabelinya dengan metode canggih."""
    all_sheets = source_spreadsheet.worksheets()
    exclude_sheets = ["DATABASE", "DATABASE_BRAND", "kamus_brand", "DB KLIK - REKAP - READY", "DB KLIK - REKAP - HABIS"]
    df_list = []

    for sheet in all_sheets:
        if sheet.title not in exclude_sheets:
            st.write(f"Memproses sheet: {sheet.title}...")
            data = sheet.get_all_records()
            if not data: continue

            df = pd.DataFrame(data)
            parts = sheet.title.split(' - REKAP - ')
            df['Toko'] = parts[0].strip() if len(parts) == 2 else sheet.title
            df['Status'] = parts[1].strip() if len(parts) == 2 else 'Unknown'
            df_list.append(df)

    if not df_list:
        st.warning("Tidak ada data toko yang ditemukan untuk diproses.")
        return pd.DataFrame(), pd.DataFrame()

    combined_df = pd.concat(df_list, ignore_index=True)

    # Menerapkan fungsi deteksi canggih ke setiap baris
    results = combined_df.apply(
        lambda row: check_brand_and_category(row['NAMA'], db_df, all_brands_list),
        axis=1,
        result_type='expand'
    )
    combined_df[['BRAND_HASIL', 'KATEGORI_HASIL']] = results

    # Logika penulisan data yang sudah diperbaiki
    all_data_final = combined_df.copy()
    missing_data = all_data_final[all_data_final['BRAND_HASIL'].isna() | all_data_final['KATEGORI_HASIL'].isna()].copy()
    
    return all_data_final, missing_data

# --- Fungsi untuk Menulis Data ke Google Sheet ---
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


# --- Tampilan Aplikasi Streamlit ---
st.set_page_config(page_title="Automasi Pelabelan Brand Produk", layout="wide")
st.title("🚀 Automasi Pelabelan Brand dan Kategori Produk")

st.info("""
Aplikasi ini menggunakan metode deteksi canggih (Direct, Fuzzy, Keyword) untuk melabeli produk.
- **Spreadsheet Utama:** Berisi SEMUA produk. Produk yang tidak teridentifikasi akan memiliki kolom hasil yang kosong.
- **Spreadsheet Data Kurang:** Berisi DAFTAR produk yang perlu Anda periksa dan lengkapi secara manual.
""")

if st.button("Mulai Proses Pelabelan", type="primary"):
    with st.spinner("Menghubungi Google Sheets dan memproses data... Mohon tunggu."):
        client = get_gspread_client()
        
        st.header("1. Memuat Data Referensi")
        db_df, all_brands_list, source_spreadsheet = load_reference_data(client)

        if source_spreadsheet:
            st.success("Berhasil memuat data referensi (DATABASE & DATABASE_BRAND).")

            st.header("2. Memproses Data Toko")
            all_processed_data, missing_info_df = process_data(source_spreadsheet, db_df, all_brands_list)
            
            st.success(f"Pemrosesan selesai. Total {len(all_processed_data)} baris data diproses.")
            st.warning(f"Ditemukan {len(missing_info_df)} produk yang memerlukan pemeriksaan manual.")

            st.header("3. Menulis Hasil ke Google Sheets")
            
            # Menulis SEMUA data ke spreadsheet utama
            if not all_processed_data.empty:
                cols_to_keep = ['TANGGAL', 'NAMA', 'HARGA', 'TERJUAL/BLN', 'BRAND', 'Toko', 'Status', 'BRAND_HASIL', 'KATEGORI_HASIL']
                final_df = all_processed_data[[col for col in cols_to_keep if col in all_processed_data.columns]]
                write_to_gsheet(client, DESTINATION_SHEET_URL, "Hasil Proses Lengkap", final_df)
                st.subheader("Contoh Data yang Ditulis ke Spreadsheet Utama")
                st.dataframe(final_df.head())

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
