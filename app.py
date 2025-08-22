import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import warnings
from thefuzz import process, fuzz
import re
import time

warnings.filterwarnings('ignore', category=UserWarning, module='gspread_dataframe')

# --- Konfigurasi Google Sheets ---
SOURCE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ"
DESTINATION_SHEET_URL = "https://docs.google.com/spreadsheets/d/1RhHw8F9PN8c0_C3lBflrkBBO89BuFe5hvYRPJd8vH5c"
MISSING_INFO_SHEET_URL = "https://docs.google.com/spreadsheets/d/1Tu7hUiV7ZRijKLQWxWOVmv81ussqoPfKlkM5WFiHof0"

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

# --- FUNGSI PEMUATAN DATA (SUDAH EFISIEN) ---
@st.cache_data(show_spinner=False)
def load_all_data(_client, _status_container):
    try:
        _status_container.write("🔄 Membuka koneksi ke Google Sheets...")
        source_spreadsheet = _client.open_by_url(SOURCE_SHEET_URL)
        
        _status_container.write("📚 Membaca data referensi (DATABASE, BRAND, KAMUS)...")
        db_df = pd.DataFrame(source_spreadsheet.worksheet("DATABASE").get_all_records())
        db_brand_df = pd.DataFrame(source_spreadsheet.worksheet("DATABASE_BRAND").get_all_records())
        kamus_df = pd.DataFrame(source_spreadsheet.worksheet("kamus_brand").get_all_records())

        all_sheets = source_spreadsheet.worksheets()
        exclude_sheets = ["DATABASE", "DATABASE_BRAND", "kamus_brand", "DB KLIK - REKAP - READY", "DB KLIK - REKAP - HABIS"]
        df_list = []
        
        sheets_to_process = [s for s in all_sheets if s.title not in exclude_sheets]
        for sheet in sheets_to_process:
            _status_container.write(f"🚚 Memuat data dari `{sheet.title}`...")
            data = sheet.get_all_records()
            if not data: continue
            df = pd.DataFrame(data)
            parts = sheet.title.split(' - REKAP - ')
            df['Toko'] = parts[0].strip() if len(parts) == 2 else sheet.title
            df['Status'] = parts[1].strip() if len(parts) == 2 else 'Unknown'
            df_list.append(df)
        
        if not df_list: return None, None, None, None
            
        combined_df = pd.concat(df_list, ignore_index=True)
        return combined_df, db_df, db_brand_df, kamus_df
    except Exception as e:
        st.error(f"Gagal memuat data: {e}")
        return None, None, None, None

# --- FUNGSI DETEKSI CANGGIHAN (UNTUK SISA DATA) ---
def find_best_match_for_remaining(product_name, db_df, all_brands_list):
    # Fungsi ini sekarang hanya untuk fuzzy dan keyword
    if not isinstance(product_name, str) or not product_name.strip():
        return None, None
    product_name_lower = product_name.lower().strip()
    
    # Fuzzy Match
    choices = db_df['NAMA'].dropna().str.lower()
    if not choices.empty:
        result = process.extractOne(product_name_lower, choices, scorer=fuzz.token_sort_ratio)
        if result is not None:
            match, score = result[0], result[1]
            if score >= 90:
                matched_row = db_df[db_df['NAMA'].str.lower() == match]
                if not matched_row.empty:
                    return matched_row.iloc[0]['Brand'], matched_row.iloc[0]['Kategori']

    # Keyword Search
    for brand in all_brands_list:
        if re.search(r'\b' + re.escape(brand.lower()) + r'\b', product_name_lower):
            return brand.upper(), None
    return None, None

# --- Fungsi untuk Menulis Data ke Google Sheet ---
def write_to_gsheet(client, sheet_url, worksheet_name, df_to_write):
    # (Fungsi ini tidak perlu diubah)
    try:
        spreadsheet = client.open_by_url(sheet_url)
        worksheet = spreadsheet.worksheet(worksheet_name)
        worksheet.clear()
        set_with_dataframe(worksheet, df_to_write, resize=True)
        st.success(f"Berhasil menulis {len(df_to_write)} baris ke sheet '{worksheet_name}'.")
    except Exception as e:
        st.error(f"Gagal menulis data ke sheet '{worksheet_name}': {e}")

# --- Tampilan Aplikasi Streamlit ---
st.set_page_config(page_title="Automasi Pelabelan Efisien", layout="wide")
st.title("🚀 Automasi Pelabelan Brand dan Kategori (Versi Efisien)")
st.info("Aplikasi ini menggunakan metode pemrosesan yang dioptimalkan untuk menangani data dalam jumlah besar dengan cepat.")

if st.button("Mulai Proses Pelabelan", type="primary"):
    client = get_gspread_client()
    
    with st.status("Langkah 1: Memuat semua data...", expanded=True) as status:
        data_toko, db_df, db_brand_df, kamus_df = load_all_data(client, status)
        if data_toko is not None:
            status.update(label="✅ Data berhasil dimuat!", state="complete", expanded=False)
        else:
            status.update(label="❌ Gagal memuat data.", state="error")

    if data_toko is not None:
        with st.status("Langkah 2: Melakukan pelabelan data...", expanded=True) as status:
            
            status.write("🔧 Mempersiapkan data referensi...")
            db_df.dropna(subset=['NAMA'], inplace=True)
            all_brands_list = db_brand_df.iloc[:, 0].dropna().str.strip().unique().tolist()
            kamus_dict = dict(zip(kamus_df['Alias'], kamus_df['Brand_Utama']))
            
            status.write("🧹 Membersihkan brand awal menggunakan `kamus_brand`...")
            data_toko['BRAND_CLEANED'] = data_toko.get('BRAND', pd.Series(dtype='str')).astype(str).str.strip().replace(kamus_dict)
            
            # --- OPTIMASI 1: PENCOCOKAN LANGSUNG (SUPER CEPAT) ---
            status.write(f"⚡ Melakukan pencocokan langsung pada {len(data_toko)} baris...")
            db_subset = db_df[['NAMA', 'Brand', 'Kategori']].rename(columns={'Brand': 'BRAND_DB', 'Kategori': 'KATEGORI_DB'})
            processed_data = pd.merge(data_toko, db_subset, on='NAMA', how='left')
            
            # --- OPTIMASI 2: ISOLASI SISA DATA ---
            remaining_df = processed_data[processed_data['BRAND_DB'].isna()].copy()
            status.write(f"🎯 Mengisolasi {len(remaining_df)} baris data yang perlu pemeriksaan lebih lanjut...")
            
            if not remaining_df.empty:
                status.write("🧠 Menerapkan deteksi canggih (Fuzzy & Keyword) pada sisa data...")
                # Terapkan fungsi yang lebih lambat HANYA pada sisa data
                remaining_results = remaining_df.apply(
                    lambda row: find_best_match_for_remaining(row.get('NAMA', ''), db_df, all_brands_list),
                    axis=1,
                    result_type='expand'
                )
                # Gabungkan hasilnya kembali ke DataFrame utama
                processed_data.loc[remaining_df.index, 'BRAND_DB'] = remaining_results[0]
                processed_data.loc[remaining_df.index, 'KATEGORI_DB'] = remaining_results[1]

            status.write("🧩 Menggabungkan semua hasil pelabelan...")
            processed_data['BRAND_HASIL'] = processed_data['BRAND_DB'].fillna(processed_data['BRAND_CLEANED'])
            processed_data['KATEGORI_HASIL'] = processed_data['KATEGORI_DB']

            all_processed_data = processed_data
            missing_info_df = all_processed_data[all_processed_data['BRAND_HASIL'].isna() | all_processed_data['KATEGORI_HASIL'].isna()].copy()
            status.update(label="✅ Pelabelan Selesai!", state="complete", expanded=False)

        st.success(f"Pemrosesan selesai. Total {len(all_processed_data)} baris data diproses.")
        st.warning(f"Ditemukan {len(missing_info_df)} produk yang memerlukan pemeriksaan manual.")

        with st.status("Langkah 3: Menulis hasil ke Google Sheets...", expanded=True):
            cols_to_keep = ['TANGGAL', 'NAMA', 'HARGA', 'TERJUAL/BLN', 'BRAND', 'Toko', 'Status', 'BRAND_HASIL', 'KATEGORI_HASIL']
            final_df = all_processed_data[[col for col in cols_to_keep if col in all_processed_data.columns]]
            write_to_gsheet(client, DESTINATION_SHEET_URL, "Hasil Proses Lengkap", final_df)
            
            if not missing_info_df.empty:
                cols_to_keep_missing = ['TANGGAL', 'NAMA', 'Toko', 'Status']
                final_missing_df = missing_info_df[[col for col in cols_to_keep_missing if col in missing_info_df.columns]]
                write_to_gsheet(client, MISSING_INFO_SHEET_URL, "Perlu Dicek Manual", final_missing_df)
