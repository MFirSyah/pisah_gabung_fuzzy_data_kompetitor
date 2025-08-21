# =============================================================================
# APLIKASI STREAMLIT UNTUK ANALISIS & EKSPOR DATA OTOMATIS
# =============================================================================
# Deskripsi:
# Membaca data dari satu Google Sheet, memprosesnya (fuzzy match, kategori),
# lalu secara otomatis menulis hasil bersihnya ke Google Sheet lain.
# =============================================================================

# --- Import Library ---
import streamlit as st
import pandas as pd
from thefuzz import process
import gspread
from google.oauth2.service_account import Credentials
import io

# =============================================================================
# KONFIGURASI HALAMAN STREAMLIT
# =============================================================================
st.set_page_config(layout="wide", page_title="Otomatisasi Data untuk Looker")

st.title("⚙️ Otomatisasi Data: G-Sheet ke G-Sheet")
st.markdown("""
Aplikasi ini akan membaca data mentah dari Spreadsheet sumber, melakukan semua proses pembersihan dan standardisasi, lalu **menuliskan hasilnya secara langsung** ke Spreadsheet tujuan yang siap untuk Looker Studio.
""")

# =============================================================================
# FUNGSI-FUNGSI UTAMA
# =============================================================================

# --- Fungsi Menginisialisasi Koneksi ke Google Sheets ---
def init_gspread_client():
    """Menginisialisasi dan mengembalikan klien gspread yang sudah terautentikasi."""
    # PERUBAHAN PENTING: Menambahkan scope 'drive' untuk izin yang lebih luas
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=scopes
    )
    client = gspread.authorize(creds)
    return client

# --- Fungsi Mengunduh Data dari Google Sheets Sumber ---
@st.cache_data(ttl=600, show_spinner="Menghubungkan ke G-Sheet sumber & mengambil data mentah...")
def load_source_data(_client):
    """Membaca semua sheet dari Spreadsheet sumber dan menggabungkannya."""
    try:
        source_url = "https://docs.google.com/spreadsheets/d/1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ"
        workbook = _client.open_by_url(source_url)
        
        sheets = workbook.worksheets()
        rekap_dfs = []
        db_master_df = None

        for sheet in sheets:
            if "REKAP" in sheet.title:
                data = sheet.get_all_records()
                df = pd.DataFrame(data)
                parts = sheet.title.split(' - REKAP - ')
                if len(parts) == 2:
                    df['Toko'] = parts[0].strip()
                    df['Status'] = 'Ready' if parts[1].strip().startswith('RE') else 'Habis'
                    rekap_dfs.append(df)
            elif "DATABASE" in sheet.title and "BRAND" not in sheet.title:
                db_master_df = pd.DataFrame(sheet.get_all_records())

        if not rekap_dfs or db_master_df is None:
            st.error("Pastikan Spreadsheet sumber memiliki sheet 'REKAP' dan 'DATABASE'.")
            return None, None
            
        combined_df = pd.concat(rekap_dfs, ignore_index=True)
        db_master_df.drop_duplicates(subset=['NAMA'], inplace=True)
        return combined_df, db_master_df

    except Exception as e:
        st.error(f"Gagal membaca data dari G-Sheet sumber. Error: {e}")
        return None, None

# --- Fungsi untuk Memproses Data ---
def process_data(df, db_master):
    """Melakukan fuzzy matching dan semua langkah pembersihan data."""
    # (Fungsi ini sama seperti sebelumnya, tidak ada perubahan)
    with st.spinner("Melakukan standardisasi nama produk dan kategori..."):
        df['NAMA'] = df['NAMA'].astype(str)
        db_master['NAMA'] = db_master['NAMA'].astype(str)
        master_product_list = db_master['NAMA'].tolist()
        master_category_map = pd.Series(db_master.Kategori.values, index=db_master.NAMA).to_dict()

        def find_master_data(product_name):
            if not product_name: return product_name, 'Lain-lain'
            best_match, score = process.extractOne(product_name, master_product_list)
            if score >= 85:
                category = master_category_map.get(best_match, 'Lain-lain')
                return best_match, category
            else:
                return product_name, 'Lain-lain'

        match_results = df['NAMA'].apply(find_master_data).apply(pd.Series)
        match_results.columns = ['Nama Produk Master', 'Kategori']
        processed_df = pd.concat([df.reset_index(drop=True), match_results.reset_index(drop=True)], axis=1)

    with st.spinner("Finalisasi data..."):
        for col in ['HARGA', 'TERJUAL/BLN']:
            processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce').fillna(0)
        processed_df['TANGGAL'] = pd.to_datetime(processed_df['TANGGAL'], errors='coerce')
        processed_df.dropna(subset=['TANGGAL'], inplace=True)
        processed_df['Minggu'] = processed_df['TANGGAL'].dt.strftime('%Y-W%U')
        processed_df.rename(columns={'NAMA': 'Nama Produk (Asli)', 'BRAND': 'Brand (Asli)', 'TERJUAL/BLN': 'Terjual/Bln'}, inplace=True)
        final_columns_order = ['TANGGAL', 'Minggu', 'Toko', 'Status', 'Nama Produk (Asli)', 'Nama Produk Master', 'Kategori', 'Harga', 'Terjual/Bln', 'Brand (Asli)']
        final_df = processed_df[[col for col in final_columns_order if col in processed_df.columns]]
    return final_df

# --- FUNGSI BARU: Menulis Data ke Google Sheets Tujuan ---
def write_data_to_gsheet(_client, df, sheet_url):
    """Menulis DataFrame ke sheet tujuan, menimpa data yang sudah ada."""
    with st.spinner(f"Menulis {len(df)} baris data ke G-Sheet tujuan..."):
        try:
            workbook = _client.open_by_url(sheet_url)
            worksheet = workbook.worksheet("Sheet1") # Menargetkan sheet bernama "Sheet1"
            
            # Membersihkan sheet sebelum menulis data baru
            worksheet.clear()
            
            # Menulis header dan data
            worksheet.update([df.columns.values.tolist()] + df.values.tolist())
            
            st.success("✅ Data berhasil ditulis ke Spreadsheet tujuan!")
            st.markdown(f"Anda bisa melihat hasilnya di sini: [Link ke Google Sheet]({sheet_url})")

        except gspread.exceptions.WorksheetNotFound:
            st.error("Error: Sheet dengan nama 'Sheet1' tidak ditemukan di Spreadsheet tujuan. Mohon buat terlebih dahulu.")
        except Exception as e:
            st.error(f"Gagal menulis data ke G-Sheet tujuan. Pastikan Anda sudah memberikan akses 'Editor'.")
            st.error(f"Detail error: {e}")

# =============================================================================
# TAMPILAN APLIKASI
# =============================================================================

# Tombol utama untuk menjalankan seluruh alur kerja
if st.button("Jalankan Proses: Baca -> Bersihkan -> Tulis ke G-Sheet", type="primary"):
    client = init_gspread_client()
    data_mentah, db_master = load_source_data(client)
    
    if data_mentah is not None and db_master is not None:
        st.info(f"Berhasil memuat {len(data_mentah)} baris data mentah.")
        final_data = process_data(data_mentah, db_master)
        
        # Tampilkan preview data hasil proses
        st.header("📊 Preview Hasil Data yang Telah Diproses")
        st.dataframe(final_data)
        st.info(f"Total baris data setelah dibersihkan: **{len(final_data)}**")

        # URL Spreadsheet tujuan Anda
        target_sheet_url = "https://docs.google.com/spreadsheets/d/1RhHw8F9PN8c0_C3lBflrkBBO89BuFe5hvYRPJd8vH5c"
        
        # Menjalankan fungsi untuk menulis data
        write_data_to_gsheet(client, final_data, target_sheet_url)
