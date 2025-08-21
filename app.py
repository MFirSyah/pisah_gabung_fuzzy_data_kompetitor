# =============================================================================
# APLIKASI STREAMLIT UNTUK ANALISIS DATA KOMPETITOR
# =============================================================================
# Deskripsi:
# Aplikasi ini mengambil data mentah dari Google Sheets, membersihkannya,
# melakukan standardisasi nama produk dan kategori menggunakan fuzzy matching,
# lalu menyajikan hasilnya untuk diunduh.
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
Aplikasi ini dirancang untuk mengotomatisasi proses pembersihan dan pengayaan data penjualan dari berbagai toko yang tersimpan di Google Sheets. 
Prosesnya meliputi penggabungan data, standardisasi nama produk, dan penambahan kategori secara otomatis.
""")

# =============================================================================
# FUNGSI-FUNGSI UTAMA
# =============================================================================

# --- Fungsi untuk Mengunduh Data dari Google Sheets ---
# Menggunakan cache agar tidak perlu mengunduh data berulang kali jika tidak ada perubahan
@st.cache_data(ttl=600, show_spinner="Menghubungkan ke Google Sheets dan mengambil data...")
def load_data_from_gsheets():
    """
    Menghubungkan ke Google API, membuka Spreadsheet, dan membaca semua sheet
    yang relevan menjadi DataFrame.
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
        all_data_list = []
        db_master_df = None
        
        # Memisahkan sheet rekap dan sheet database
        rekap_sheets = [s for s in sheets if "REKAP" in s.title]
        database_sheet = next((s for s in sheets if "DATABASE" in s.title), None)

        if not rekap_sheets:
            st.error("Tidak ada sheet dengan nama 'REKAP' yang ditemukan di Google Sheets.")
            return None, None
            
        if not database_sheet:
            st.error("Sheet 'DATABASE' tidak ditemukan. Sheet ini wajib ada sebagai kamus.")
            return None, None

        # Membaca semua sheet rekap
        for sheet in rekap_sheets:
            data = sheet.get_all_records()
            df = pd.DataFrame(data)
            
            # Menambahkan informasi Toko dan Status dari judul sheet
            parts = sheet.title.split(' - REKAP - ')
            df['Toko'] = parts[0].strip()
            df['Status'] = 'Ready' if parts[1].strip().startswith('RE') else 'Habis'
            all_data_list.append(df)
            
        # Membaca sheet database
        db_master_df = pd.DataFrame(database_sheet.get_all_records())
        db_master_df.drop_duplicates(subset=['NAMA'], inplace=True)
        
        combined_df = pd.concat(all_data_list, ignore_index=True)
        return combined_df, db_master_df

    except Exception as e:
        st.error(f"Gagal terhubung atau membaca data dari Google Sheets. Pastikan Anda sudah mengatur `secrets.toml` dengan benar.")
        st.error(f"Detail error: {e}")
        return None, None

# --- Fungsi untuk Memproses Data ---
def process_data(df, db_master):
    """
    Melakukan fuzzy matching untuk standardisasi nama produk dan kategorisasi.
    """
    with st.spinner("Melakukan standardisasi nama produk dan kategori (fuzzy matching)..."):
        master_product_list = db_master['NAMA'].tolist()
        master_category_map = pd.Series(db_master.Kategori.values, index=db_master.NAMA).to_dict()

        def find_master_data(product_name):
            if not isinstance(product_name, str) or not product_name:
                return product_name, 'Lain-lain'
            
            best_match, score = process.extractOne(product_name, master_product_list)
            
            if score >= 85: # Ambang batas kemiripan
                category = master_category_map.get(best_match, 'Lain-lain')
                return best_match, category
            else:
                return product_name, 'Lain-lain'

        match_results = df['NAMA'].apply(find_master_data).apply(pd.Series)
        match_results.columns = ['Nama Produk Master', 'Kategori']
        
        processed_df = pd.concat([df, match_results], axis=1)

    with st.spinner("Menambahkan kolom waktu dan finalisasi..."):
        # Konversi kolom numerik
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

if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None

# Tombol untuk memulai proses
if st.button("🚀 Mulai Proses Analisis", type="primary"):
    data_mentah, db_master = load_data_from_gsheets()
    
    if data_mentah is not None and db_master is not None:
        st.success(f"Berhasil memuat {len(data_mentah)} baris data mentah.")
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
    # Mengonversi DataFrame ke CSV di dalam memori
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
