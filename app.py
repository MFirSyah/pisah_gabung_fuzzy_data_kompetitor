# =============================================================================
# DASHBOARD ANALISIS KOMPETITOR ONLINE
# Dibuat untuk: Firman
# Sumber Data: Google Sheets
# Fitur:
# - Penggabungan data otomatis dari semua sheet
# - Standardisasi nama produk & kategori dengan Fuzzy Matching
# - Dasbor interaktif dengan filter
# - Caching untuk performa cepat
# =============================================================================

# --- Import Library ---
import streamlit as st
import pandas as pd
from thefuzz import process
import plotly.express as px
import gspread
from google.oauth2.service_account import Credentials
import time

# --- Konfigurasi Halaman ---
st.set_page_config(layout="wide", page_title="Dashboard Analisis Kompetitor")

# --- Fungsi untuk Koneksi ke Google Sheets ---
# Menggunakan caching agar tidak perlu koneksi ulang setiap kali ada interaksi
@st.cache_resource(ttl=3600) # Cache resource selama 1 jam
def connect_to_gsheets():
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_dict = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        client = gspread.authorize(creds)
        st.success("Berhasil terkoneksi ke Google Sheets!")
        return client
    except Exception as e:
        st.error(f"Gagal koneksi ke Google Sheets. Cek konfigurasi `secrets.toml`. Error: {e}")
        return None

# --- Fungsi Utama untuk Memuat dan Memproses Data ---
# Menggunakan caching data agar proses fuzzy matching yang berat tidak diulang-ulang
@st.cache_data(ttl=3600, show_spinner="Memuat dan memproses data dari semua toko...") # Cache data selama 1 jam
def load_and_process_data(_client, spreadsheet_url):
    if _client is None:
        return pd.DataFrame()

    try:
        # Buka spreadsheet
        spreadsheet = _client.open_by_url(spreadsheet_url)
        
        # Ambil semua sheet, kecuali yang bernama 'DATABASE'
        worksheets = [ws for ws in spreadsheet.worksheets() if "DATABASE" not in ws.title.upper() and "KAMUS" not in ws.title.upper()]
        
        all_data = []
        progress_bar = st.progress(0, text="Membaca data dari setiap sheet...")
        
        for i, ws in enumerate(worksheets):
            # Cek jika sheet berisi data rekap
            if "REKAP" in ws.title.upper():
                file_name = f"DATA_REKAP.xlsx - {ws.title}"
                parts = file_name.replace('DATA_REKAP.xlsx - ', '').replace('.csv', '').split(' - REKAP - ')
                store_name = parts[0].strip()
                status_part = parts[1].strip()
                status = 'Ready' if status_part.startswith('RE') else 'Habis'
                
                df = pd.DataFrame(ws.get_all_records())
                df['Toko'] = store_name
                df['Status'] = status
                all_data.append(df)
            progress_bar.progress((i + 1) / len(worksheets), text=f"Membaca sheet: {ws.title}")

        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Memuat DATABASE sebagai acuan
        db_sheet = spreadsheet.worksheet("DATABASE")
        db_master = pd.DataFrame(db_sheet.get_all_records())
        db_master.drop_duplicates(subset=['NAMA'], inplace=True)

        # Proses Fuzzy Matching
        progress_bar.progress(0.7, text="Melakukan Fuzzy Matching untuk produk dan kategori...")
        master_product_list = db_master['NAMA'].tolist()
        master_category_map = pd.Series(db_master.Kategori.values, index=db_master.NAMA).to_dict()

        def find_master_data(product_name):
            if not isinstance(product_name, str): return product_name, 'Lain-lain'
            best_match, score = process.extractOne(product_name, master_product_list)
            if score >= 85:
                category = master_category_map.get(best_match, 'Lain-lain')
                return best_match, category
            return product_name, 'Lain-lain'

        match_results = combined_df['NAMA'].apply(find_master_data).apply(pd.Series)
        match_results.columns = ['Nama Produk Master', 'Kategori']
        combined_df = pd.concat([combined_df, match_results], axis=1)

        # Konversi Tipe Data & Buat Kolom Minggu
        progress_bar.progress(0.9, text="Finalisasi data...")
        combined_df['TANGGAL'] = pd.to_datetime(combined_df['TANGGAL'])
        combined_df['Minggu'] = combined_df['TANGGAL'].dt.strftime('%Y-W%U')
        combined_df['Harga'] = pd.to_numeric(combined_df['Harga'], errors='coerce')

        progress_bar.empty()
        return combined_df

    except Exception as e:
        st.error(f"Terjadi error saat memproses data: {e}")
        return pd.DataFrame()


# =============================================================================
# TAMPILAN APLIKASI (UI)
# =============================================================================

st.title("📊 Dashboard Analisis Kompetitor")
st.markdown("Dashboard ini mengambil data secara *live* dari Google Sheets dan melakukan analisis secara otomatis.")

# --- Hubungkan ke Gsheets dan Proses Data ---
spreadsheet_url = "https://docs.google.com/spreadsheets/d/1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ/edit#gid=197904951"
client = connect_to_gsheets()
df = load_and_process_data(client, spreadsheet_url)


if not df.empty:
    # --- Sidebar untuk Filter ---
    st.sidebar.header("Filter Data:")
    
    selected_toko = st.sidebar.multiselect(
        "Pilih Toko:",
        options=df['Toko'].unique(),
        default=df['Toko'].unique()
    )

    selected_kategori = st.sidebar.multiselect(
        "Pilih Kategori:",
        options=sorted(df['Kategori'].unique()),
        default=sorted(df['Kategori'].unique())
    )

    min_date = df['TANGGAL'].min().date()
    max_date = df['TANGGAL'].max().date()
    selected_date = st.sidebar.date_input(
        "Pilih Rentang Tanggal:",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    # Filter DataFrame berdasarkan pilihan
    df_filtered = df[
        (df['Toko'].isin(selected_toko)) &
        (df['Kategori'].isin(selected_kategori)) &
        (df['TANGGAL'].dt.date >= selected_date[0]) &
        (df['TANGGAL'].dt.date <= selected_date[1])
    ]

    # --- Tampilan Utama ---
    
    # KPI (Key Performance Indicators)
    total_produk_unik = df_filtered['Nama Produk Master'].nunique()
    jumlah_toko = df_filtered['Toko'].nunique()
    rata_harga = df_filtered['Harga'].mean()

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Produk Unik", f"{total_produk_unik:,}")
    col2.metric("Jumlah Toko Terfilter", f"{jumlah_toko}")
    col3.metric("Rata-rata Harga", f"Rp {rata_harga:,.0f}")

    st.markdown("---")

    # Grafik
    st.header("Visualisasi Data")
    
    col_grafik1, col_grafik2 = st.columns(2)
    
    with col_grafik1:
        # Grafik 1: Jumlah Produk per Toko
        produk_per_toko = df_filtered.groupby('Toko')['Nama Produk Master'].nunique().sort_values(ascending=False).reset_index()
        fig_toko = px.bar(
            produk_per_toko,
            x='Toko',
            y='Nama Produk Master',
            title='Jumlah Produk Unik per Toko',
            labels={'Nama Produk Master': 'Jumlah Produk Unik', 'Toko': 'Nama Toko'},
            text_auto=True
        )
        st.plotly_chart(fig_toko, use_container_width=True)

    with col_grafik2:
        # Grafik 2: Distribusi Kategori
        kategori_dist = df_filtered['Kategori'].value_counts().reset_index()
        fig_kategori = px.pie(
            kategori_dist.head(10), # Ambil top 10
            names='Kategori',
            values='count',
            title='Top 10 Distribusi Kategori Produk',
            hole=0.3
        )
        st.plotly_chart(fig_kategori, use_container_width=True)

    # Menampilkan Data Tabel
    st.header("Detail Data")
    st.dataframe(df_filtered)

else:
    st.warning("Data tidak berhasil dimuat. Periksa koneksi dan konfigurasi.")