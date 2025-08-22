import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import warnings

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

# --- Fungsi untuk Memuat Data ---
def load_reference_data(client):
    """Memuat sheet referensi (DATABASE, kamus, dll.) ke dalam DataFrame."""
    try:
        source_spreadsheet = client.open_by_url(SOURCE_SHEET_URL)
        db_df = pd.DataFrame(source_spreadsheet.worksheet("DATABASE").get_all_records())
        kamus_df = pd.DataFrame(source_spreadsheet.worksheet("kamus_brand").get_all_records())
        db_brand_df = pd.DataFrame(source_spreadsheet.worksheet("DATABASE_BRAND").get_all_records())
        
        # Mengambil hanya kolom pertama dari DATABASE_BRAND
        if not db_brand_df.empty:
            db_brand_df = db_brand_df.iloc[:, [0]].rename(columns={db_brand_df.columns[0]: 'Brand'})
            
        return db_df, kamus_df, db_brand_df, source_spreadsheet
    except Exception as e:
        st.error(f"Gagal memuat data referensi: {e}")
        return None, None, None, None

# --- Fungsi Utama untuk Memproses Data ---
def process_data(source_spreadsheet, db_df, kamus_df, db_brand_df):
    """Fungsi utama untuk menggabungkan, membersihkan, dan melabeli data."""
    all_sheets = source_spreadsheet.worksheets()
    exclude_sheets = ["DATABASE", "DATABASE_BRAND", "kamus_brand", "DB KLIK - REKAP - READY", "DB KLIK - REKAP - HABIS"]
    
    df_list = []
    
    kamus_dict = dict(zip(kamus_df['Alias'], kamus_df['Brand_Utama']))

    for sheet in all_sheets:
        if sheet.title not in exclude_sheets:
            st.write(f"Memproses sheet: {sheet.title}...")
            data = sheet.get_all_records()
            if not data:
                continue

            df = pd.DataFrame(data)
            
            # Ekstrak info Toko dan Status dari nama sheet
            parts = sheet.title.split(' - REKAP - ')
            if len(parts) == 2:
                df['Toko'] = parts[0].strip()
                df['Status'] = parts[1].strip()
            else:
                df['Toko'] = sheet.title
                df['Status'] = 'Unknown'

            # Ganti nama kolom 'TERJUAL/BLN' jika ada spasi ekstra
            if 'TERJUAL/ BLN' in df.columns:
                df.rename(columns={'TERJUAL/ BLN': 'TERJUAL/BLN'}, inplace=True)

            df_list.append(df)

    if not df_list:
        st.warning("Tidak ada data toko yang ditemukan untuk diproses.")
        return pd.DataFrame(), pd.DataFrame()

    combined_df = pd.concat(df_list, ignore_index=True)

    # 1. Bersihkan brand menggunakan kamus_brand
    combined_df['BRAND_CLEANED'] = combined_df['BRAND'].str.strip().replace(kamus_dict)
    
    # 2. Lakukan merge dengan DATABASE (prioritas utama)
    # Ganti nama kolom di db_df untuk menghindari konflik saat merge
    db_df.rename(columns={'Brand': 'BRAND_DB', 'Kategori': 'KATEGORI_DB'}, inplace=True)
    merged_df = pd.merge(combined_df, db_df[['NAMA', 'BRAND_DB', 'KATEGORI_DB']], on='NAMA', how='left')

    # 3. Buat kolom final untuk Brand dan Kategori
    # Prioritas: Brand dari DATABASE, jika tidak ada, gunakan brand yang sudah dibersihkan
    merged_df['BRAND_FINAL'] = merged_df['BRAND_DB'].fillna(merged_df['BRAND_CLEANED'])
    merged_df['KATEGORI_FINAL'] = merged_df['KATEGORI_DB']

    # Validasi dengan DATABASE_BRAND jika brand masih kosong
    # Jika BRAND_FINAL masih kosong, coba cari dari NAMA produk
    brand_list = db_brand_df['Brand'].str.lower().tolist()
    def find_brand_in_name(product_name):
        if not isinstance(product_name, str):
            return None
        for brand in brand_list:
            if f" {brand} " in f" {product_name.lower()} ":
                return brand.upper()
        return None

    merged_df['BRAND_FINAL'] = merged_df['BRAND_FINAL'].fillna(merged_df['NAMA'].apply(find_brand_in_name))
    
    # Pisahkan data yang lengkap dan yang tidak
    valid_data = merged_df[merged_df['BRAND_FINAL'].notna() & merged_df['KATEGORI_FINAL'].notna()].copy()
    missing_data = merged_df[merged_df['BRAND_FINAL'].isna() | merged_df['KATEGORI_FINAL'].isna()].copy()
    
    return valid_data, missing_data

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
        # Jika sheet tidak ada, buat sheet baru
        st.warning(f"Worksheet '{worksheet_name}' tidak ditemukan. Membuat sheet baru...")
        spreadsheet.add_worksheet(title=worksheet_name, rows=100, cols=20)
        worksheet = spreadsheet.worksheet(worksheet_name)
        set_with_dataframe(worksheet, df_to_write, resize=True)
        st.success(f"Berhasil membuat dan menulis {len(df_to_write)} baris ke sheet '{worksheet_name}'.")
    except Exception as e:
        st.error(f"Gagal menulis data ke sheet '{worksheet_name}': {e}")


# --- Tampilan Aplikasi Streamlit ---
st.set_page_config(page_title="Automasi Pelabelan Brand Produk", layout="wide")
st.title("🚀 Automasi Pelabelan Brand dan Kategori Produk")

st.info("""
Aplikasi ini akan membaca data dari Google Sheet sumber, melakukan pelabelan ulang brand dan kategori,
lalu menuliskan hasilnya ke dua Google Sheet tujuan yang berbeda.
- **Data Berhasil:** Produk dengan brand DAN kategori yang ditemukan.
- **Data Kurang:** Produk yang brand ATAU kategorinya tidak ditemukan.
""")

if st.button("Mulai Proses Pelabelan", type="primary"):
    with st.spinner("Menghubungi Google Sheets dan memproses data... Mohon tunggu."):
        client = get_gspread_client()
        
        st.header("1. Memuat Data Referensi")
        db_df, kamus_df, db_brand_df, source_spreadsheet = load_reference_data(client)

        if source_spreadsheet:
            st.success("Berhasil memuat data referensi (DATABASE, kamus_brand, DATABASE_BRAND).")

            st.header("2. Memproses Data Toko")
            processed_df, missing_info_df = process_data(source_spreadsheet, db_df, kamus_df, db_brand_df)
            
            st.success(f"Pemrosesan selesai. Ditemukan {len(processed_df)} data valid dan {len(missing_info_df)} data kurang.")

            st.header("3. Menulis Hasil ke Google Sheets")
            if not processed_df.empty:
                # Membersihkan kolom sebelum menulis
                cols_to_keep_processed = ['TANGGAL', 'NAMA', 'HARGA', 'TERJUAL/BLN', 'BRAND', 'Toko', 'Status', 'BRAND_FINAL', 'KATEGORI_FINAL']
                processed_df_final = processed_df[[col for col in cols_to_keep_processed if col in processed_df.columns]].copy()
                processed_df_final.rename(columns={'BRAND_FINAL': 'BRAND_HASIL', 'KATEGORI_FINAL': 'KATEGORI_HASIL'}, inplace=True)
                write_to_gsheet(client, DESTINATION_SHEET_URL, "Hasil Proses", processed_df_final)
                st.subheader("Contoh Data yang Berhasil Diproses")
                st.dataframe(processed_df_final.head())
            else:
                st.warning("Tidak ada data yang berhasil diproses untuk ditulis.")

            if not missing_info_df.empty:
                # Menyiapkan kolom sesuai permintaan
                missing_info_df.rename(columns={'NAMA': 'NAMA PRODUK', 'TANGGAL':'TANGGAL'}, inplace=True)
                cols_to_keep_missing = ['TANGGAL', 'NAMA PRODUK', 'Toko', 'Status']
                missing_info_df_final = missing_info_df[[col for col in cols_to_keep_missing if col in missing_info_df.columns]].copy()
                write_to_gsheet(client, MISSING_INFO_SHEET_URL, "Data Kurang", missing_info_df_final)
                st.subheader("Contoh Data dengan Informasi Kurang")
                st.dataframe(missing_info_df_final.head())
            else:
                st.info("Luar biasa! Tidak ada data dengan informasi kurang ditemukan.")
