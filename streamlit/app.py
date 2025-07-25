import streamlit as st
import pandas as pd
import altair as alt
import os

# streamlit config
def config_streamlit():
    st.set_page_config(
        page_title="Breweries Dashboard",
        page_icon="🍻",
        layout="wide",
    )

    m = st.markdown("""
    <style>
    div.stButton > button:first-child{
        width: 100%;
        font-size: 18px;
    }
                                    
    div.block-container{
        padding-top: 2rem;
        padding-left: 2rem;
        padding-right: 2rem;
        padding-bottom: 2rem;
    }

    </style>""", unsafe_allow_html=True)

@st.cache_data(ttl=600)
def get_gold_data_df(object_key: str) -> pd.DataFrame:
    """
    Reads the partitioned Parquet data from the Gold layer in Minio and returns a DataFrame.
    """
    print("--- Reading data from Gold Layer ---")
    
    # Get S3 configuration from environment variables
    try:
        s3_endpoint_url = os.environ.get("S3_ENDPOINT_URL")
        s3_access_key = os.environ.get("S3_ACCESS_KEY")
        s3_secret_key = os.environ.get("S3_SECRET_KEY")
        gold_bucket = os.environ.get("GOLD_BUCKET")
    except KeyError as e:
        print(f"Could not find required environment variable: {e}")
        raise

    storage_options = {
        "key": s3_access_key,
        "secret": s3_secret_key,
        "client_kwargs": {"endpoint_url": s3_endpoint_url}
    }

    object_path = f"s3://{gold_bucket}/{object_key}"
    print(f"Reading partitioned Parquet data from {object_path}")
    
    try:
        # Ensure you have 'pyarrow' and 's3fs' installed: pip install pyarrow s3fs
        df = pd.read_parquet(object_path, storage_options=storage_options)
        print(f"Successfully read {len(df)} records from the Gold layer.")
        return df
    except Exception as e:
        st.error(f"Failed to read data from Minio. Error: {e}")
        print(f"Failed to read data from Gold layer. Error: {e}")
        return pd.DataFrame()


if __name__ == '__main__':
    config_streamlit()

    st.title("🍻 Brewery Analysis Dashboard")

    # read the parquet files from the gold layer
    df_agg = get_gold_data_df("breweries_by_type_and_location.parquet")
    df_dup = get_gold_data_df("breweries_with_duplicate_names.parquet")


    
    # Ensure df_agg is not empty before proceeding
    if df_agg.empty:
        st.error("Failed to load the main data file (breweries_by_type_and_location.parquet). Dashboard cannot be rendered.")
    else:
        col1, col2 = st.columns([7, 3])

        # Aggregated data
        with col1:
            with st.expander('Full Aggregated data content'):
                st.dataframe(df_agg, use_container_width=True, hide_index=True)

            # View 1: Top 20 breweries by country and type
            st.header("Top 20 Brewery & Country Combinations")
            # Prepare data for the top 20 chart by getting the 20 largest counts
            top_20_df = df_agg.nlargest(20, 'count').copy()
            # Create a descriptive label for the x-axis
            top_20_df['display_label'] = top_20_df['country'].astype(str) + ' - ' + top_20_df['brewery_type'].astype(str)

            # Create a bar chart showing each unique combination
            bar_chart = alt.Chart(top_20_df).mark_bar().encode(
                # X-axis: The combined label, sorted by the count in descending order.
                x=alt.X('display_label:N',
                        sort='-y', # This sorts the bars by the y-value (the count)
                        title="Country & Brewery Type"),

                # Y-axis: The count for that specific combination
                y=alt.Y('count:Q', title="Brewery Count"),

                # Color: Differentiate bars by brewery_type
                color=alt.Color('brewery_type:N', title="Brewery Type"),

                # Tooltip: Show details on hover
                tooltip=['country', 'brewery_type', 'count']
            )

            # Create the text layer to display values on top of the bars
            text_layer = alt.Chart(top_20_df).mark_text(
                align='center',
                baseline='bottom',
                dy=-5,  # Nudge text up slightly
                color='black'
            ).encode(
                x=alt.X('display_label:N', sort='-y'),
                y=alt.Y('count:Q'),
                text=alt.Text('count:Q', format="d") # Format as integer
            )

            # Layer the bar chart and the text layer
            overall_chart = (bar_chart + text_layer).properties(
                height=300
            ).interactive() # Allow zooming and panning

            # Display the chart in Streamlit
            st.altair_chart(overall_chart, use_container_width=True)

            # View 2: Breweries by country
            # --- Sidebar Filters for Second Chart ---
            st.sidebar.header("Detailed View Filters")
            
            # Get a list of unique countries for the filter
            all_countries = sorted(df_agg['country'].unique())

            # Create a multiselect widget for countries for the second chart
            selected_countries = st.sidebar.multiselect(
                "Select Countries for Detailed View",
                options=all_countries,
                default=['England', 'Ireland', 'Scotland'] # Pre-select some countries
            )

            # Filter the dataframe based on selection.
            if selected_countries:
                filtered_df = df_agg[df_agg['country'].isin(selected_countries)]
            else:
                # If no selection, create an empty dataframe to show nothing
                filtered_df = pd.DataFrame()


            # --- Filtered Chart Creation ---
            st.header("Detailed Brewery Counts by Type")
            st.markdown("Use the filter in the sidebar to select countries. This chart compares brewery counts for each type, with a separate bar for each country.")

            if not filtered_df.empty:
                # Create a grouped bar chart using the filtered dataframe
                filtered_bar_chart = alt.Chart(filtered_df).mark_bar().encode(
                    # X-axis: Brewery Type
                    x=alt.X('brewery_type:N', title="Brewery Type"),

                    # Y-axis: The count for that specific combination
                    y=alt.Y('count:Q', title="Brewery Count"),

                    # Color: Differentiate bars by country
                    color=alt.Color('country:N', title="Country"),
                    
                    # Grouping: Create separate bars for each country within a brewery type category
                    xOffset='country:N',

                    # Tooltip: Show details on hover
                    tooltip=['country', 'brewery_type', 'count']
                )

                # Create the text layer for the filtered chart
                filtered_text_layer = alt.Chart(filtered_df).mark_text(
                    align='center',
                    baseline='bottom',
                    dy=-5,
                    color='black'
                ).encode(
                    x=alt.X('brewery_type:N'),
                    y=alt.Y('count:Q'),
                    text=alt.Text('count:Q', format='d'),
                    # Use xOffset to align the text with the correct bar in the group
                    xOffset='country:N'
                )

                # Layer the bar chart and the text layer
                filtered_chart = (filtered_bar_chart + filtered_text_layer).properties(
                    height=300
                ).interactive()

                # Display the filtered chart in Streamlit
                st.altair_chart(filtered_chart, use_container_width=True)
            
            # --- Display Raw Data ---
            with st.expander("Show Filtered Data Table"):
                if not filtered_df.empty:
                    st.dataframe(filtered_df)
                else:
                    st.write("No countries selected.")


            # --- Breweries by Type Chart ---
            st.header("Total Breweries by Type")
            st.markdown("This chart shows the total count of breweries for each type across all countries.")

            # Aggregate data by brewery type
            brewery_type_agg_df = df_agg.groupby('brewery_type')['count'].sum().reset_index()

            # Create the bar chart
            type_bar_chart = alt.Chart(brewery_type_agg_df).mark_bar().encode(
                x=alt.X('brewery_type:N', sort='-y', title="Brewery Type"),
                y=alt.Y('count:Q', title="Total Brewery Count"),
                tooltip=['brewery_type', 'count'],
                color=alt.Color('brewery_type:N', title="Brewery Type")
            )

            # Create the text layer
            type_text_layer = alt.Chart(brewery_type_agg_df).mark_text(
                align='center',
                baseline='bottom',
                dy=-5,
                color='black'
            ).encode(
                x=alt.X('brewery_type:N', sort='-y'),
                y=alt.Y('count:Q'),
                text=alt.Text('count:Q', format='d')
            )

            # Layer and display the chart
            type_chart = (type_bar_chart + type_text_layer).properties(
                height=300
            ).interactive()

            st.altair_chart(type_chart, use_container_width=True)

        # Duplicate breweries
        with col2:
            if not df_dup.empty:
                with st.expander('Full Duplicate breweries content'):
                    st.dataframe(df_dup, use_container_width=True, hide_index=True)
                
                # --- Top 10 Duplicate Breweries Chart ---
                st.markdown("### Top 10 Breweries by Number of Locations")

                # Count occurrences of each brewery name
                dup_counts = df_dup['name'].value_counts().reset_index()
                dup_counts.columns = ['name', 'location_count']
                
                # Get the top 10
                top_10_dups = dup_counts.nlargest(10, 'location_count')

                # Create the horizontal bar chart
                dup_bar_chart = alt.Chart(top_10_dups).mark_bar().encode(
                    y=alt.Y('name:N', sort='-x', title="Brewery Name"),
                    x=alt.X('location_count:Q', title="Number of Locations"),
                    tooltip=['name', 'location_count']
                )

                # Create the text layer for the bars
                dup_text_layer = alt.Chart(top_10_dups).mark_text(
                    align='left',
                    baseline='middle',
                    dx=3,  # Nudge text to the right of the bar
                    color='black'
                ).encode(
                    y=alt.Y('name:N', sort='-x'),
                    x=alt.X('location_count:Q'),
                    text=alt.Text('location_count:Q', format='d')
                )

                # Layer and display the chart
                dup_chart = (dup_bar_chart + dup_text_layer).properties(
                    height=300
                ).interactive()

                st.altair_chart(dup_chart, use_container_width=True)

            else:
                st.write("No duplicate brewery data loaded.")

