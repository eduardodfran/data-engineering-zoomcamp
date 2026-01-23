import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
import time
import math

st.set_page_config(
    page_title="Data Ingestion Tool",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Data Ingestion Tool")
st.markdown("---")

# Sidebar for configuration
st.sidebar.header("Database Configuration")
pg_user = st.sidebar.text_input("Postgres User", value="root")
pg_password = st.sidebar.text_input("Postgres Password", value="root", type="password")
pg_host = st.sidebar.text_input("Postgres Host", value="pgdatabase")
pg_port = st.sidebar.text_input("Postgres Port", value="5432")
pg_db = st.sidebar.text_input("Postgres Database", value="ny_taxi")

st.sidebar.markdown("---")
st.sidebar.header("Ingestion Settings")
chunk_size = st.sidebar.number_input("Chunk Size", min_value=1000, max_value=100000, value=50000, step=5000)

# Main content
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Data Source")
    
    # Data source options
    source_type = st.radio("Select Data Source Type:", ["URL", "File Upload"], horizontal=True)
    
    if source_type == "URL":
        url = st.text_input(
            "Data URL",
            value="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
            help="Enter the URL to your CSV or Parquet file"
        )
        file_data = None
    else:
        uploaded_file = st.file_uploader("Upload CSV or Parquet file", type=['csv', 'parquet'])
        url = None
        file_data = uploaded_file

    table_name = st.text_input("Table Name", value="taxi_data", help="Name of the table in the database")

with col2:
    st.subheader("Quick Stats")
    if st.session_state.get('last_ingestion'):
        stats = st.session_state['last_ingestion']
        st.metric("Rows Ingested", stats.get('rows', 'N/A'))
        st.metric("Time Taken", f"{stats.get('time', 0):.2f}s")
        st.metric("Status", stats.get('status', 'N/A'))

st.markdown("---")

# Ingestion button
if st.button("🚀 Start Ingestion", type="primary", use_container_width=True):
    if (source_type == "URL" and not url) or (source_type == "File Upload" and not file_data):
        st.error("⚠️ Please provide a data source!")
    elif not table_name:
        st.error("⚠️ Please provide a table name!")
    else:
        try:
            # Create progress indicators
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            start_time = time.time()
            
            # Create database engine
            status_text.text("Connecting to database...")
            engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')
            progress_bar.progress(10)
            
            # Determine file type and read data
            is_parquet = False
            if source_type == "URL":
                is_parquet = url.endswith('.parquet')
                file_source = url
            else:
                is_parquet = file_data.name.endswith('.parquet')
                file_source = file_data
            
            if is_parquet:
                status_text.text("Reading Parquet file...")
                df = pd.read_parquet(file_source)
                progress_bar.progress(50)
                
                status_text.text("Creating table schema...")
                df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
                progress_bar.progress(60)
                
                # Insert in chunks to avoid memory issues
                total_rows = len(df)
                parquet_chunk_size = 5000
                total_chunks = max(1, math.ceil(total_rows / parquet_chunk_size))
                status_text.text(f"Inserting chunks: 0/{total_chunks} (0%)")
                
                for chunk_index in range(total_chunks):
                    start_idx = chunk_index * parquet_chunk_size
                    end_idx = min(start_idx + parquet_chunk_size, total_rows)
                    chunk_df = df.iloc[start_idx:end_idx]
                    chunk_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
                    del chunk_df
                    percent = int(((chunk_index + 1) / total_chunks) * 100)
                    status_text.text(f"Inserting chunks: {chunk_index + 1}/{total_chunks} ({percent}%)")
                    progress_bar.progress(min(60 + int(((chunk_index + 1) / total_chunks) * 40), 100))
                
                progress_bar.progress(100)
                
            else:
                status_text.text("Counting rows for progress...")
                total_rows = 0
                count_iter = pd.read_csv(file_source, usecols=[0], chunksize=chunk_size)
                for count_chunk in count_iter:
                    total_rows += len(count_chunk)
                total_chunks = max(1, math.ceil(total_rows / chunk_size))

                if source_type == "File Upload":
                    file_source.seek(0)

                status_text.text("Reading CSV file...")
                df_iter = pd.read_csv(file_source, iterator=True, chunksize=chunk_size)
                
                # First chunk
                df = next(df_iter)
                progress_bar.progress(20)
                
                status_text.text("Creating table schema...")
                df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
                progress_bar.progress(30)
                
                status_text.text(f"Inserting chunk 1/{total_chunks} (0%)")
                df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
                ingested_rows = len(df)
                
                # Process remaining chunks
                chunk_count = 1
                for df in df_iter:
                    chunk_count += 1
                    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
                    ingested_rows += len(df)
                    percent = int((chunk_count / total_chunks) * 100)
                    status_text.text(f"Inserting chunk {chunk_count}/{total_chunks} ({percent}%)")
                    progress_bar.progress(min(30 + int((chunk_count / total_chunks) * 70), 100))
                
                progress_bar.progress(100)
            
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            # Close engine connection
            engine.dispose()
            
            # Store stats
            st.session_state['last_ingestion'] = {
                'rows': total_rows,
                'time': elapsed_time,
                'status': '✅ Success'
            }
            
            status_text.empty()
            progress_bar.empty()
            
            st.success(f"✅ Successfully ingested {total_rows:,} rows into table '{table_name}' in {elapsed_time:.2f} seconds!")
            
            # Show sample data (only first 10 rows to save memory)
            st.subheader("Sample Data Preview")
            sample_df = df.head(10).copy()
            st.dataframe(sample_df, use_container_width=True)
            
            # Clean up memory
            del df
            if 'sample_df' in locals():
                del sample_df
            import gc
            gc.collect()
            
        except Exception as e:
            st.error(f"❌ Error during ingestion: {str(e)}")
            import traceback
            st.error(traceback.format_exc())
            st.session_state['last_ingestion'] = {
                'rows': 0,
                'time': 0,
                'status': '❌ Failed'
            }
            # Clean up on error
            try:
                engine.dispose()
            except:
                pass

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: gray;'>
        <p>Data Engineering Zoomcamp - Data Ingestion Tool</p>
    </div>
    """,
    unsafe_allow_html=True
)
