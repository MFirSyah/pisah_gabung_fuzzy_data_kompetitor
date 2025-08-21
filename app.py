# =============================================================================
# APLIKASI STREAMLIT UNTUK ANALISIS DATA KOMPETITOR (VERSI ADAPTASI)
# =============================================================================
# Deskripsi:
# Menggunakan metode pengambilan data yang diadaptasi dari skrip referensi
# untuk menggabungkan, membersihkan, dan mengayakan data dari Google Sheets.
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
st.set_page_config(layout="wide", page_title="Dashboard Analisis Data")

st.title("🚀 Aplikasi Analisis Data Kompetitor")
st.markdown("""
Aplikasi ini mengambil data mentah dari Google Sheets, membersihkannya, dan melakukan standardisasi nama produk serta kategori secara otomatis.
Gunakan tombol di bawah untuk memulai proses.
""")

# =============================================================================
# FUNGSI-FUNGSI UTAMA
# =============================================================================

# --- Fungsi Mengunduh Data dari Google Sheets (METODE ADAPTASI) ---
@st.cache_data(ttl=600, show_spinner="Menghubungkan ke Google Sheets dan mengambil data...")
def load_data_from_gsheets_adapted():
    """
    Menghubungkan ke Google API dan membaca SEMUA sheet dalam satu kali proses,
    lalu memisahkannya menjadi data rekap dan database master.
    """
    try:
        # Menggunakan st.secrets untuk autentikasi yang aman
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"], scopes=scopes
        )
        client = gspread.authorize(creds)
        
        spreadsheet_url = "https://docs.google.com/spreadsheets/d/1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ"
        workbook = client.open_by_url(spreadsheet_url)
        
        sheets = workbook.worksheets()
        all_dfs = {}
        for sheet in sheets:
            data = sheet.get_all_records()
            all_dfs[sheet.title] = pd.DataFrame(data)

        # Memisahkan data rekap dan database dari dictionary yang sudah dibuat
        rekap_dfs = []
        db_master_df = None

        for sheet_name, df in all_dfs.items():
            if "REKAP" in sheet_name:
                parts = sheet_name.split(' - REKAP - ')
                if len(parts) == 2:
                    df['Toko'] = parts[0].strip()
                    df['Status'] = 'Ready' if parts[1].strip().startswith('RE') else 'Habis'
                    rekap_dfs.append(df)
            elif "DATABASE" in sheet_name and "BRAND" not in sheet_name:
                db_master_df = df

        if not rekap_dfs:
            st.error("Tidak ada sheet dengan nama 'REKAP' yang ditemukan.")
            return None, None
            
        if db_master_df is None:
            st.error("Sheet 'DATABASE' tidak ditemukan. Sheet ini wajib ada sebagai kamus.")
            return None, None

        combined_df = pd.concat(rekap_dfs, ignore_index=True)
        db_master_df.drop_duplicates(subset=['NAMA'], inplace=True)
        
        return combined_df, db_master_df

    except Exception as e:
        st.error(f"Gagal terhubung atau membaca data dari Google Sheets. Pastikan Anda sudah mengatur `secrets.toml`.")
        st.error(f"Detail error: {e}")
        return None, None

# --- Fungsi untuk Memproses Data ---
def process_data(df, db_master):
    """
    Melakukan fuzzy matching untuk standardisasi nama produk dan kategorisasi.
    """
    with st.spinner("Melakukan standardisasi nama produk dan kategori (fuzzy matching)..."):
        # Membersihkan kolom 'NAMA' dari nilai non-string
        df['NAMA'] = df['NAMA'].astype(str)
        db_master['NAMA'] = db_master['NAMA'].astype(str)

        master_product_list = db_master['NAMA'].tolist()
        master_category_map = pd.Series(db_master.Kategori.values, index=db_master.NAMA).to_dict()

        def find_master_data(product_name):
            if not product_name:
                return product_name, 'Lain-lain'
            
            best_match, score = process.extractOne(product_name, master_product_list)
            
            if score >= 85: # Ambang batas kemiripan
                category = master_category_map.get(best_match, 'Lain-lain')
                return best_match, category
            else:
                return product_name, 'Lain-lain'

        match_results = df['NAMA'].apply(find_master_data).apply(pd.Series)
        match_results.columns = ['Nama Produk Master', 'Kategori']
        
        processed_df = pd.concat([df.reset_index(drop=True), match_results.reset_index(drop=True)], axis=1)

    with st.spinner("Menambahkan kolom waktu dan finalisasi..."):
        # Konversi kolom numerik, mengatasi nilai kosong atau non-numerik
        for col in ['HARGA', 'TERJUAL/BLN']:
            processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce').fillna(0)

        # Proses kolom tanggal
        processed_df['TANGGAL'] = pd.to_datetime(processed_df['TANGGAL'], errors='coerce')
        processed_df.dropna(subset=['TANGGAL'], inplace=True)
        processed_df['Minggu'] = processed_df['TANGGAL'].dt.strftime('%Y-W%U')

        # Mengatur ulang nama dan urutan kolom
        processed_df.rename(columns={'NAMA': 'Nama Produk (Asli)', 'BRAND': 'Brand (Asli)', 'TERJUAL/BLN': 'Terjual/Bln'}, inplace=True)
        
        final_columns_order = [
            'TANGGAL', 'Minggu', 'Toko', 'Status', 'Nama Produk (Asli)', 
            'Nama Produk Master', 'Kategori', 'Harga', 'Terjual/Bln', 'Brand (Asli)'
        ]
        
        final_df = processed_df[[col for col in final_columns_order if col in processed_df.columns]]

    return final_df

# =============================================================================
# TAMPILAN APLIKASI
# =============================================================================

# Menggunakan session_state untuk menyimpan data yang sudah diproses
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None

# Tombol untuk memulai proses
if st.button("🚀 Mulai Proses & Ambil Data", type="primary"):
    data_mentah, db_master = load_data_from_gsheets_adapted()
    
    if data_mentah is not None and db_master is not None:
        st.success(f"Berhasil memuat {len(data_mentah)} baris data mentah dari semua sheet rekap.")
        final_data = process_data(data_mentah, db_master)
        st.session_state.processed_data = final_data
        st.success("🎉 Semua data berhasil diproses!")
    else:
        st.warning("Proses tidak dapat dilanjutkan karena gagal memuat data.")

# Menampilkan hasil jika data sudah diproses
if st.session_state.processed_data is not None:
    final_df = st.session_state.processed_data
    
    st.header("📊 Hasil Data yang Telah Diproses")
    st.dataframe(final_df)
    st.info(f"Total baris data setelah dibersihkan: **{len(final_df)}**")

    # --- Fitur Download ---
    @st.cache_data
    def convert_df_to_csv(df):
        return df.to_csv(index=False).encode('utf-8')

    csv_data = convert_df_to_csv(final_df)

    st.download_button(
       label="📥 Download Hasil Data (CSV)",
       data=csv_data,
       file_name='data_hasil_analisis.csv',
       mime='text/csv',
    )
