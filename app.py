# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR
#  Metode Pemeriksaan Adaptif
# ===================================================================================

# ===================================================================================
# IMPORT LIBRARY
# ===================================================================================
import streamlit as st
import pandas as pd
from thefuzz import process, fuzz
import gspread
from google.oauth2.service_account import Credentials
import warnings

warnings.filterwarnings('ignore', category=UserWarning, module='gspread_dataframe')

# ===================================================================================
# KONFIGURASI HALAMAN & URL
# ===================================================================================
st.set_page_config(layout="wide", page_title="Dashboard Analisis")

# Ganti dengan URL Google Sheet Anda
SOURCE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ"
DESTINATION_SHEET_URL = "https://docs.google.com/spreadsheets/d/1RhHw8F9PN8c0_C3lBflrkBBO89BuFe5hvYRPJd8vH5c"
MISSING_INFO_SHEET_URL = "https://docs.google.com/spreadsheets/d/1Tu7hUiV7ZRijKLQWxWOVmv81ussqoPfKlkM5WFiHof0"


# ===================================================================================
# FUNGSI-FUNGSI UTAMA
# ===================================================================================

# --- Fungsi Autentikasi ke Google Sheets ---
@st.cache_resource
def get_gspread_client():
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

# --- FUNGSI BARU: Pemeriksaan Brand & Kategori Adaptif ---
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
    # Menggunakan 'token_sort_ratio' yang andal untuk urutan kata yang berbeda
    match, score = process.extractOne(product_name_lower, database_df['NAMA'].str.lower(), scorer=fuzz.token_sort_ratio)
    if score >= 90: # Batas skor kemiripan 90%
        matched_row = database_df[database_df['NAMA'].str.lower() == match]
        if not matched_row.empty:
            return matched_row.iloc[0]['Brand'], matched_row.iloc[0]['Kategori']

    # --- Langkah 3: Keyword Search ---
    for brand in all_brands_list:
        # Mencari brand sebagai kata utuh (dikelilingi spasi/awal/akhir)
        if re.search(r'\b' + re.escape(brand.lower()) + r'\b', product_name_lower):
            return brand, "Belum Diketahui" # Kategori tidak bisa ditentukan dari sini

    # --- Jika semua gagal ---
    return None, None


# --- Fungsi Pemrosesan Data yang Diadaptasi ---
@st.cache_data(show_spinner="Memproses dan melabeli data...")
def process_and_label_data_adapted(_client):
    """Fungsi utama untuk memuat, menggabungkan, dan memproses data dengan metode baru."""
    source_spreadsheet = _client.open_by_url(SOURCE_SHEET_URL)
    
    # 1. Muat Data Referensi
    db_df = pd.DataFrame(source_spreadsheet.worksheet("DATABASE").get_all_records())
    db_brand_df = pd.DataFrame(source_spreadsheet.worksheet("DATABASE_BRAND").get_all_records())
    
    # Pastikan kolom referensi tidak kosong dan bersihkan
    db_df.dropna(subset=['NAMA', 'Brand', 'Kategori'], inplace=True)
    all_brands_list = db_brand_df.iloc[:, 0].dropna().unique().tolist()
    
    # 2. Muat dan Gabungkan Data Toko
    all_sheets = source_spreadsheet.worksheets()
    exclude_sheets = ["DATABASE", "DATABASE_BRAND", "kamus_brand"] # Kita tidak perlu kamus lagi
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
    combined_df.rename(columns={'NAMA': 'Nama Produk'}, inplace=True)

    # 3. Terapkan Fungsi Pemeriksaan Adaptif
    # Ini adalah bagian inti dari adaptasi
    st.write("Menerapkan metode pemeriksaan adaptif pada setiap produk...")
    results = combined_df.apply(
        lambda row: check_brand_and_category(row['Nama Produk'], db_df, all_brands_list),
        axis=1,
        result_type='expand'
    )
    combined_df[['BRAND_HASIL', 'KATEGORI_HASIL']] = results

    # 4. Pisahkan Data
    valid_data = combined_df[combined_df['BRAND_HASIL'].notna() & combined_df['KATEGORI_HASIL'].notna()].copy()
    missing_data = combined_df[combined_df['BRAND_HASIL'].isna() | combined_df['KATEGORI_HASIL'].isna()].copy()
    
    return valid_data, missing_data

# --- Fungsi untuk Menulis Data ke Google Sheet ---
def write_to_gsheet(client, sheet_url, worksheet_name, df_to_write):
    # (Fungsi ini bisa dibiarkan sama seperti kode Anda sebelumnya)
    try:
        spreadsheet = client.open_by_url(sheet_url)
        worksheet = spreadsheet.worksheet(worksheet_name)
        worksheet.clear()
        set_with_dataframe(worksheet, df_to_write, resize=True)
        st.success(f"Berhasil menulis {len(df_to_write)} baris ke sheet '{worksheet_name}'.")
    except gspread.exceptions.WorksheetNotFound:
        st.warning(f"Worksheet '{worksheet_name}' tidak ditemukan. Membuat sheet baru...")
        worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=len(df_to_write)+1, cols=len(df_to_write.columns))
        set_with_dataframe(worksheet, df_to_write, resize=True)
        st.success(f"Berhasil membuat dan menulis {len(df_to_write)} baris ke sheet '{worksheet_name}'.")
    except Exception as e:
        st.error(f"Gagal menulis data ke sheet '{worksheet_name}': {e}")


# ===================================================================================
# TAMPILAN APLIKASI STREAMLIT
# ===================================================================================
st.title("🚀 Automasi Pelabelan Brand & Kategori (Metode Adaptif)")

st.info("""
Aplikasi ini menggunakan metode 3-langkah (Direct, Fuzzy, Keyword) untuk melabeli brand dan kategori.
Klik tombol di bawah untuk memulai proses.
""")

if st.button("Mulai Proses Pelabelan Adaptif", type="primary"):
    with st.spinner("Menjalankan proses... Ini mungkin memakan waktu beberapa saat."):
        client = get_gspread_client()
        
        st.header("1. Memproses Data")
        processed_df, missing_info_df = process_and_label_data_adapted(client)
        st.success(f"Pemrosesan selesai. Ditemukan {len(processed_df)} data valid dan {len(missing_info_df)} data kurang.")

        st.header("2. Menulis Hasil ke Google Sheets")
        if not processed_df.empty:
            cols_to_keep = ['TANGGAL', 'Nama Produk', 'HARGA', 'BRAND', 'Toko', 'Status', 'BRAND_HASIL', 'KATEGORI_HASIL']
            final_df = processed_df[[col for col in cols_to_keep if col in processed_df.columns]]
            write_to_gsheet(client, DESTINATION_SHEET_URL, "Hasil Proses Adaptif", final_df)
            st.subheader("Contoh Data yang Berhasil Diproses")
            st.dataframe(final_df.head())

        if not missing_info_df.empty:
            cols_to_keep_missing = ['TANGGAL', 'Nama Produk', 'Toko', 'Status']
            final_missing_df = missing_info_df[[col for col in cols_to_keep_missing if col in missing_info_df.columns]]
            write_to_gsheet(client, MISSING_INFO_SHEET_URL, "Data Kurang (Adaptif)", final_missing_df)
            st.subheader("Contoh Data dengan Informasi Kurang")
            st.dataframe(final_missing_df.head())
        else:
            st.info("Luar biasa! Tidak ada data dengan informasi kurang ditemukan.")
