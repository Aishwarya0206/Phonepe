import streamlit as st
import pydeck as pdk
import time
import numpy as np
import pandas as pd
import altair as alt
import plotly.express as plotly
from urllib.error import URLError
from dataHandler import dataHandler
from data_mapping_with_transfer import data_mapping_with_transfer

def intro():
    #st.markdown(page_title="Hello",page_icon="👋")
    #st.markdown(f"# {list(page_names_to_funcs.keys())[0]}")
    st.write("# Welcome to PhonePe Pulse! 👋")

    st.markdown(
        """
        The Indian digital payments story has truly captured the world's imagination. From the largest towns to the remotest villages, there is a payments revolution being driven by the penetration of mobile phones and data.

        When PhonePe started 5 years back, we were constantly looking for definitive data sources on digital payments in India. Some of the questions we were seeking answers to were - How are consumers truly using digital payments? What are the top cases? Are kiranas across Tier 2 and 3 getting a facelift with the penetration of QR codes?
        This year as we became India's largest digital payments platform with 46% UPI market share, we decided to demystify the what, why and how of digital payments in India.

        This year, as we crossed 2000 Cr. transactions and 30 Crore registered users, we thought as India's largest digital payments platform with 46% UPI market share, we have a ring-side view of how India sends, spends, manages and grows its money. So it was time to demystify and share the what, why and how of digital payments in India.

        PhonePe Pulse is your window to the world of how India transacts with interesting trends, deep insights and in-depth analysis based on our data put together by the PhonePe team.
    """
    )

def card_layout(title, content):
    st.write(
        f"""
        <div style="border: 1px solid #e6e6e6; border-radius: 1px; padding: 1px; margin-bottom: 1px;">
            <h3 style="margin-bottom: 1px; text-align: center;">{title}{content}</h3>
        </div>
        <br/>
        """,
        unsafe_allow_html=True,
    )

def card_layout_categories(content):
    print(content)
    st.write(
        f"""
        <div style="border: 1px solid #e6e6e6; border-radius: 5px; padding: 20px; margin-bottom: 20px;">
            <h3 style="margin-bottom: 10px; text-align: center;">Categories</h3>
            <p style="text-align: center;">Recharge & bill payments : {content[2]}</p>
            <p style="text-align: center;">Peer-to-peer payments    : {content[1]}</p>
            <p style="text-align: center;">Merchant payments        : {content[0]}</p>
            <p style="text-align: center;">Financial Services       : {content[3]}</p>
            <p style="text-align: center;">Others                   : {content[4]}</p>
        </div>
        <br/>
        """,
        unsafe_allow_html=True,
    )

def payments():
    #st.markdown(f'# {list(page_names_to_funcs.keys())[1]}')
    Type=st.sidebar.selectbox(":blue[Type]",("Transactions","Users"))
    
    st.header(":blue["+Type+"]")
    Year=st.sidebar.slider(":blue[Year]",min_value=2018,max_value=2023)
    Quaters=st.sidebar.slider(":blue[Quaters]",min_value=1,max_value=4)
    connect = sql_conn.connect_db()
    
    usersByStates = sql_conn.get_user_value_by_states(connect['cursor'], connect['conn'], Year, Quaters)
    usersByDistricts = sql_conn.get_user_value_by_districts(connect['cursor'], connect['conn'], Year, Quaters)
    usersByPincodes = sql_conn.get_user_value_by_pincodes(connect['cursor'], connect['conn'], Year, Quaters)
    if(Type == 'Transactions'):
        distinctTypeOfPaymentCategory = sql_conn.distinctTypeOfPaymentCategories(connect['cursor'], connect['conn'])
        TypeOfPaymentCategory=st.sidebar.multiselect(":blue[Choose Type of payment category]", distinctTypeOfPaymentCategory, distinctTypeOfPaymentCategory)
        sum_of_transaction = sql_conn.sum_of_AggregatedTransforms(connect['cursor'], connect['conn'], Year, Quaters, ", ".join(TypeOfPaymentCategory))
        totalPaymentValue = sql_conn.total_payment_value(connect['cursor'], connect['conn'], Year, Quaters, ", ".join(TypeOfPaymentCategory))
        avgTransactionValue = sql_conn.average_transaction_value(connect['cursor'], connect['conn'], Year, Quaters, ", ".join(TypeOfPaymentCategory))
        avgTransactionValueByCategory = sql_conn.get_payment_value_by_category(connect['cursor'], connect['conn'], Year, Quaters, ", ".join(TypeOfPaymentCategory))
        getTransactionValueByStates = sql_conn.get_transaction_value_by_states(connect['cursor'], connect['conn'], Year, Quaters)
        getTransactionValueByDistricts = sql_conn.get_transaction_value_by_districts(connect['cursor'], connect['conn'], Year, Quaters)
        getTransactionValueByPincodes = sql_conn.get_transaction_value_by_pincodes(connect['cursor'], connect['conn'], Year, Quaters)
        card_layout("All PhonePe transactions: ", str(format(int(sum_of_transaction), ',')))
        card_layout("Total payment value: ", "₹ "+str(format(int(totalPaymentValue), ',')))
        card_layout("Avg. transaction value: ", "₹ "+str(format(int(avgTransactionValue), ',')))
        st.dataframe(avgTransactionValueByCategory)
        tab1, tab2, tab3 = st.tabs([":blue[States]", ":blue[Districts]", ":blue[Postal Codes]"])
        
        with tab1:
            st.dataframe(getTransactionValueByStates)

        with tab2:
            st.dataframe(getTransactionValueByDistricts)

        with tab3:
            st.dataframe(getTransactionValueByPincodes)

    if(Type == 'Users'):
        sum_of_users = sql_conn.sum_of_AggregatedUsers(connect['cursor'], connect['conn'], Year, Quaters)
        sum_of_AppOpenUsers = sql_conn.sum_of_AppOpenUsers(connect['cursor'], connect['conn'], Year, Quaters)
        card_layout(f"Registered PhonePe users till Q{Quaters} {Year}: ", str(format(int(sum_of_users), ',')))
        card_layout(f"PhonePe app opens in Q{Quaters} {Year}: ", str(format(int(sum_of_AppOpenUsers), ',')) if(int(sum_of_AppOpenUsers) != 0) else "Unavailable")
        tab4, tab5, tab6 = st.tabs([":blue[States]", ":blue[Districts]", ":blue[Postal Codes]"])
        with tab4:
            st.dataframe(usersByStates)

        with tab5:
            st.dataframe(usersByDistricts)

        with tab6:
            st.dataframe(usersByPincodes)
    

    # Streamlit widgets automatically run the script from top to bottom. Since
    # this button is not connected to any other logic, it just causes a plain
    # rerun.
    st.button("Re-run")

def mapping_demo():
    Type = st.sidebar.selectbox(":blue[Type]", ("Transactions", "Users"))
    Year=st.sidebar.slider(":blue[Year]",min_value=2018,max_value=2023)
    Quaters=st.sidebar.slider(":blue[Quaters]",min_value=1,max_value=4)
    getTransactionValueByStates = sql_conn.get_top_transaction_by_states(connect['cursor'], connect['conn'], Year, Quaters)
    getTransactionValueByDistricts = sql_conn.get_top_transaction_by_districts(connect['cursor'], connect['conn'], Year, Quaters)
    getTransactionValueByPincodes = sql_conn.get_top_transaction_by_pincodes(connect['cursor'], connect['conn'], Year, Quaters)
    usersByStates = sql_conn.get_user_value_by_states(connect['cursor'], connect['conn'], Year, Quaters)
    usersByDistricts = sql_conn.get_user_value_by_districts(connect['cursor'], connect['conn'], Year, Quaters)
    usersByPincodes = sql_conn.get_user_value_by_pincodes(connect['cursor'], connect['conn'], Year, Quaters)
    usersByDevices = sql_conn.get_user_value_by_devices(connect['cursor'], connect['conn'], Year, Quaters)
    if Type == "Transactions":
        tab1, tab2, tab3 = st.tabs([":blue[States]", ":blue[Districts]", ":blue[Postal Codes]"])
        with tab1:
            fig = plotly.pie(getTransactionValueByStates,
                            values='TotalTransactionValue',
                            names='State',
                            title='Top 10 transaction by states',
                            color_discrete_sequence=plotly.colors.sequential.Viridis,
                            hover_data=['NoOfTransactions'],
                            labels={'NoOfTransactions':'NoOfTransactions'})
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig,use_container_width=True)

            fig=plotly.bar(getTransactionValueByStates,
                            x="TotalTransactionValue",
                            y="State",
                            orientation="h",
                            color="NoOfTransactions",
                            color_discrete_sequence=plotly.colors.sequential.Viridis)

            st.plotly_chart(fig,use_container_width=True)
        with tab2:
            fig = plotly.pie(getTransactionValueByDistricts,
                            values='TotalTransactionValue',
                            names='Districts',
                            title='Top 10 transaction by districts',
                            color_discrete_sequence=plotly.colors.sequential.Viridis,
                            hover_data=['NoOfTransactions'],
                            labels={'NoOfTransactions':'NoOfTransactions'})
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig,use_container_width=True)

            fig=plotly.bar(getTransactionValueByDistricts,
                            x="TotalTransactionValue",
                            y="Districts",
                            orientation="h",
                            color="NoOfTransactions",
                            color_discrete_sequence=plotly.colors.sequential.Viridis)

            st.plotly_chart(fig,use_container_width=True)
        with tab3:
            fig = plotly.pie(getTransactionValueByPincodes,
                            values='TotalTransactionValue',
                            names='Pincodes',
                            title='Top 10 transaction by pincodes',
                            color_discrete_sequence=plotly.colors.sequential.Viridis,
                            hover_data=['NoOfTransactions'],
                            labels={'NoOfTransactions':'NoOfTransactions'})
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig,use_container_width=True)

            
    if Type == "Users":
        tab1, tab2, tab3, tab4 = st.tabs([":blue[States]", ":blue[Districts]", ":blue[Postal Codes]", ":blue[Brand]"])
        with tab1:
            fig = plotly.pie(usersByStates,
                            values='Count of users',
                            names='State',
                            title='Top 10 users by states',
                            color_discrete_sequence=plotly.colors.sequential.Viridis,
                            hover_data=['Count of users'],
                            labels={'Count of users':'Count of users'})
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig,use_container_width=True)

            fig=plotly.bar(usersByStates,
                            x="Count of users",
                            y="State",
                            orientation="h",
                            color="Count of users",
                            color_discrete_sequence=plotly.colors.sequential.Viridis)

            st.plotly_chart(fig,use_container_width=True)
        with tab2:
            fig = plotly.pie(usersByDistricts,
                            values='Count of users',
                            names='Districts',
                            title='Top 10 users by districts',
                            color_discrete_sequence=plotly.colors.sequential.Viridis,
                            hover_data=['Count of users'],
                            labels={'Count of users':'Count of users'})
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig,use_container_width=True)

            fig=plotly.bar(usersByDistricts,
                            x="Count of users",
                            y="Districts",
                            orientation="h",
                            color="Count of users",
                            color_discrete_sequence=plotly.colors.sequential.Viridis)

            st.plotly_chart(fig,use_container_width=True)
        with tab3:
            fig = plotly.pie(usersByPincodes,
                            values='Count of users',
                            names='Pincodes',
                            title='Top 10 users by pincodes',
                            color_discrete_sequence=plotly.colors.sequential.Viridis,
                            hover_data=['Count of users'],
                            labels={'Count of users':'Count of users'})
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig,use_container_width=True)
            
        with tab4:
            if isinstance(usersByDevices, pd.DataFrame) and usersByDevices.empty:
                st.write("The results are still in processing...")
            else:
                fig = plotly.pie(usersByDevices,
                                values='Registered users',
                                names='DeviceBrand',
                                title='Top 10 users by brands',
                                color_discrete_sequence=plotly.colors.sequential.Viridis,
                                hover_data=['Registered users'],
                                labels={'Registered users':'Registered users'})
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig,use_container_width=True)
                
                fig=plotly.bar(usersByDevices,
                            x="Registered users",
                            y="DeviceBrand",
                            orientation="h",
                            color="Registered users",
                            color_discrete_sequence=plotly.colors.sequential.Viridis)

                st.plotly_chart(fig,use_container_width=True)

def data_frame_demo():
    Type=st.sidebar.selectbox(":blue[Type]",("Transactions","Users"))
    
    st.header(":blue["+Type+"]")
    Year=st.sidebar.slider(":blue[Year]",min_value=2018,max_value=2023)
    Quaters=st.sidebar.slider(":blue[Quaters]",min_value=1,max_value=4)
    get_states = sql_conn.get_states(connect['cursor'], connect['conn'])
    selected_state=st.multiselect(":blue[Choose states]", get_states, get_states)
    
    if Type == "Transactions":
        tab1, tab2 = st.tabs([":blue[View 1]", ":blue[View 2]"])
        with tab1:
            st.markdown("## :green[Overall State Data - Transaction Amount]")
            df=sql_conn.get_transaction_by_states_plot(connect['cursor'], connect['conn'], Year, Quaters, ", ".join(selected_state))
            print(df)
            fig=plotly.choropleth(df,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey="properties.ST_NM",
                            locations="State",
                            color="TotalTransactionValue",
                            color_continuous_scale="sunset",
                            range_color=(min(list(df['TotalTransactionValue'])), max(list(df['TotalTransactionValue']))))
            
            fig.update_geos(fitbounds="locations",visible=False)
            st.plotly_chart(fig,use_container_width=True)
        with tab2:
            st.markdown("## :green[Overall State Data - Transaction Count]")
            df=sql_conn.get_transaction_by_states_plot(connect['cursor'], connect['conn'], Year, Quaters, ", ".join(selected_state))
            fig=plotly.choropleth(df,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey="properties.ST_NM",
                            locations="State",
                            color="NoOfTransactions",
                            color_continuous_scale="sunset",
                            range_color=(min(list(df['TotalTransactionValue'])), max(list(df['TotalTransactionValue']))))
            
            fig.update_geos(fitbounds="locations",visible=False)
            st.plotly_chart(fig,use_container_width=True)

    if Type == "Users":
        tab1, tab2 = st.tabs([":blue[View 1]", ":blue[View 2]"])
        with tab1:
            st.markdown("## :green[Overall State Data - Total Registered Users]")
            df=sql_conn.get_users_by_states_plot(connect['cursor'], connect['conn'], Year, Quaters, ", ".join(selected_state))
            fig=plotly.choropleth(df,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey="properties.ST_NM",
                            locations="State",
                            color="TotalRegisteredUsers",
                            color_continuous_scale="sunset",
                            range_color=(min(list(df['TotalRegisteredUsers'])), max(list(df['TotalRegisteredUsers']))))
            
            fig.update_geos(fitbounds="locations",visible=False)
            st.plotly_chart(fig,use_container_width=True)

        with tab2:
            st.markdown("## :green[Overall State Data - Number Of App Opens By Users]")
            df=sql_conn.get_users_by_states_plot(connect['cursor'], connect['conn'], Year, Quaters, ", ".join(selected_state))
            fig=plotly.choropleth(df,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey="properties.ST_NM",
                            locations="State",
                            color="NoOfAppOpensByUsers",
                            color_continuous_scale="sunset",
                            range_color=(min(list(df['NoOfAppOpensByUsers'])), max(list(df['NoOfAppOpensByUsers']))))
            
            fig.update_geos(fitbounds="locations",visible=False)
            st.plotly_chart(fig,use_container_width=True)

def select_stms():
    agg_transform = sql_conn.select_from_AggregatedTransforms(connect['cursor'], connect['conn'])
    agg_user = sql_conn.select_from_AggregatedUsers(connect['cursor'], connect['conn'])
    map_transform = sql_conn.select_from_MappedTransforms(connect['cursor'], connect['conn'])
    map_user = sql_conn.select_from_MappedUsers(connect['cursor'], connect['conn'])
    top_transform = sql_conn.select_from_TopTransforms(connect['cursor'], connect['conn'])
    top_user = sql_conn.select_from_TopUsers(connect['cursor'], connect['conn'])
    if(agg_transform.shape[0] == 0 and agg_user.shape[0] == 0 and  map_transform.shape[0] == 0 and map_user.shape[0] == 0 and top_transform.shape[0] == 0 and top_user.shape[0] == 0):
        return True
    else:
        return False

if __name__ == '__main__':
    # DB connection
    sql = {"host": "localhost", "user": "root", "password": "Password123#@!","database":"Phonepe"}
    sql_conn = data_mapping_with_transfer(sql)
    connect = sql_conn.connect_db()
    create_table_ddl = sql_conn.execute_ddl(connect['cursor'], connect['conn'])

    #Check for the records available in DB
    if(select_stms()):
        handler = dataHandler()
        execute_insert = handler.callToDB(sql_conn, connect)
    

    #Select from Aggregated Transform 
    agg_transform = sql_conn.select_from_AggregatedTransforms(connect['cursor'], connect['conn'])
    print(agg_transform.info())
    print(agg_transform.head())

    #Select from Aggregated Users 
    agg_user = sql_conn.select_from_AggregatedUsers(connect['cursor'], connect['conn'])
    print(agg_user.info())
    print(agg_user.head())

    #Select from Mapped Transform 
    map_transform = sql_conn.select_from_MappedTransforms(connect['cursor'], connect['conn'])
    print(map_transform.info())
    print(map_transform.head())

    #Select from Mapped Users 
    map_user = sql_conn.select_from_MappedUsers(connect['cursor'], connect['conn'])
    print(map_user.info())
    print(map_user.head())

    #Select from Top Transform 
    top_transform = sql_conn.select_from_TopTransforms(connect['cursor'], connect['conn'])
    print(top_transform.info())
    print(top_transform.head())

    #Select from Top Users 
    top_user = sql_conn.select_from_TopUsers(connect['cursor'], connect['conn'])
    print(top_user.info())
    print(top_user.head())

    #Based on the choosen page will display the contents
    page_names_to_funcs = {
        "About": intro,
        "Payments": payments,
        "Plotting": mapping_demo,
        "Visualization": data_frame_demo
    }
    app_name = st.sidebar.selectbox(":blue[Choose a Phonepe view]", page_names_to_funcs.keys())
    page_names_to_funcs[app_name]()
    