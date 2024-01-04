import streamlit as st
import pandas as pd
import datetime
from io import BytesIO
from elasticsearch import Elasticsearch

import pandas as pd
from elastic_api import search_index, search_index_with_date_range, create_data_in_bank, delete_data_in_bank, get_document_by_id
client = Elasticsearch('http://localhost:9200')


st.markdown(
    """     <style>
    [data-testid="stSidebar"][aria-expanded="true"] > div:first-child{width:250px;}     </style>
    """, unsafe_allow_html=True )
option = st.sidebar.selectbox("CRD", ("create", "read", "delete"))
if option == "create":
    st.title("bank 인덱스에 값 삽입")
elif option == "read":
    st.title("엘라스틱서치에 저장된 인덱스 조회")
else :
    st.title("bank 인덱스에 값 삭제")
clicked1, clicked2, create_button_clicked, delete_button_clicked = False, False, False, False
if option == "read":
    st.sidebar.header("조회하고 싶은 인덱스명을 입력하세요")
    index_name = st.sidebar.text_input('인덱스명', value="bank").lower()
    field_name = st.sidebar.text_input('필드명', value="location")
    match_name = st.sidebar.text_input('조회하려는 내용', value="상암")
    clicked1 = st.sidebar.button("해당 정보 확인")
    date_range = st.sidebar.date_input("도큐먼트 생성일",
                    [datetime.date(2019, 1, 1), datetime.date(2024, 1, 3)])
    clicked2 = st.sidebar.button("생성일 확인")
elif option == "create":
    st.sidebar.header("bank 인덱스에서 생성하고 싶은 정보를 입력하세요")
    date_match_name = st.sidebar.text_input('date', value="2024-01-04")
    bank_match_name = st.sidebar.text_input('bank', value="우리은행")
    branch_match_name = st.sidebar.text_input('branch', value="111호점")
    location_match_name = st.sidebar.text_input('location', value="상암")
    customers_match_name = st.sidebar.text_input('customers', value="10000")
    create_button_clicked = st.sidebar.button("해당 내용 생성")
elif option == "delete":
    st.sidebar.header("bank 인덱스에서 삭제하고 싶은 정보를 입력하세요")
    will_be_deleted = st.sidebar.text_input('삭제하실 ID를 입력하세요', value="111")
    delete_button_clicked = st.sidebar.button("해당 id의 데이터 삭제")
if create_button_clicked:
    create_data_in_bank(date_match_name, bank_match_name, branch_match_name, location_match_name, customers_match_name)
    # st.write(f"날짜 : {date_match_name}, 은행 : {bank_match_name}, 호점 : {branch_match_name}, \
    #          위치 : {location_match_name}, 고객 수 : {customers_match_name}가 생성되었습니다.")
    if create_button_clicked :
        st.write("생성이 완료 되었습니다.")
        kk = client.count(index="bank")['count']
        st.write(str(kk)+"번째 id 가 추가 되었습니다.")
        # 생성된 레코드를 확인하기 위해 조회
        document_data = get_document_by_id("bank", kk)
        st.write(document_data)
     
if delete_button_clicked:
    tt = delete_data_in_bank(will_be_deleted)
    if tt == True:
        st.write("삭제가 완료 되었습니다.")
        kk = client.count(index="bank")['count']
        st.write(will_be_deleted+"번째 id 가 삭제 되었습니다.")
        # 생성된 레코드를 확인하기 위해 조회

    else :
        st.write("없는 id 입니다. 다시 입력하세요")
        
if(clicked1 == True):
    result = search_index(index_name, field_name, match_name)
    # st.write(result.to_dict())
    st.write(result.to_dict()["hits"]["hits"])
    source_data = [entry["_source"] for entry in result.to_dict()["hits"]["hits"]]
    df = pd.DataFrame(source_data)
    st.dataframe(df)
if(clicked2 == True):
    start_p = date_range[0]
    end_p = date_range[1] + datetime.timedelta(days=1)
    result = search_index_with_date_range(index_name, field_name, match_name, start_date=start_p, end_date=end_p)
    st.write(result.to_dict()["hits"]["hits"])
    source_data = [entry["_source"] for entry in result.to_dict()["hits"]["hits"]]
    df = pd.DataFrame(source_data)
    st.dataframe(df)
    csv_data = df.to_csv()
    excel_data = BytesIO()
    df.to_excel(excel_data)
    columns = st.columns(2)
    with columns[0]:
        st.download_button("CSV 파일 다운로드", csv_data, file_name='stock_data.csv')
    with columns[1]:
        st.download_button("엑셀 파일 다운로드",
        excel_data, file_name='stock_data.xlsx')