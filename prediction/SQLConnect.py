import pymysql as sql

#development mysql server
# conn_dbInter_Formal = sql.connect(host ="rm-bp105ft9dy0qc53y8wo.mysql.rds.aliyuncs.com", port =3306, user= "v3read", passwd ="SamE57@7583jgpck", db='db_inter')
# cursor_Inter_Formal = conn_dbInter_Formal.cursor()
#
# conn_dbTrans_Formal = sql.connect(host ="rm-bp105ft9dy0qc53y8wo.mysql.rds.aliyuncs.com", port =3306, user= "v3read", passwd ="SamE57@7583jgpck", db='db_trans')
# cursor_Trans_Formal = conn_dbTrans_Formal.cursor()


try:
    conn_dbTrans = sql.connect(host ="47.99.118.183", port =3306, user= "v3dev_user", passwd ="V3dev!56", db='db_trans')
    cursor_Trans = conn_dbTrans.cursor()

    conn_dbInter = sql.connect(host ="47.99.118.183", port =3306, user= "v3dev_user", passwd ="V3dev!56", db='db_inter')
    cursor_Inter = conn_dbInter.cursor()

    conn_dbTransPlan = sql.connect(host ="47.99.118.183", port =3306, user= "v3dev_user", passwd ="V3dev!56", db='db_trans_plan')
    cursor_TransPlan = conn_dbTransPlan.cursor()

    conn_dbSys = sql.connect(host ="47.99.118.183", port =3306, user= "v3dev_user", passwd ="V3dev!56", db='db_sys')
    cursor_sys = conn_dbSys.cursor()

    conn_dbDispatch = sql.connect(host="47.99.118.183", port=3306, user="v3dev_user", passwd="V3dev!56", db='dispatch_data_analyze')
    cursor_dispatch = conn_dbDispatch.cursor()

    conn_dbSeience = sql.connect(host="47.99.118.183", port=3306, user="v3dev_user", passwd="V3dev!56", db='db_data_seience')
    cursor_seience = conn_dbSeience.cursor()
except:
    print('production mysql server failed connection')

# production copy mysql server
try:
    conn_dbInter_Copy = sql.connect(host ="172.16.30.160", port =3306, user= "v3read", passwd ="SamE57@7583jgpck", db='db_inter')
    cursor_Inter_Copy = conn_dbInter_Copy.cursor()

    conn_dbTrans_Copy = sql.connect(host ="172.16.30.160", port =3306, user= "v3read", passwd ="SamE57@7583jgpck", db='db_trans')
    cursor_trans_Copy = conn_dbTrans_Copy.cursor()

    conn_dbTransPlan_Copy = sql.connect(host ="47.99.118.183", port =3306, user= "v3dev_user", passwd ="V3dev@56", db='db_trans_plan')
    cursor_TransPlan_Copy = conn_dbTransPlan_Copy.cursor()

    conn_dbSys_Copy = sql.connect(host ="172.16.30.160", port =3306, user= "v3read", passwd ="SamE57@7583jgpck", db='db_sys')
    cursor_sys_Copy = conn_dbSys_Copy.cursor()

    dbInter_connect_info = 'mysql+pymysql://v3dev_user:V3dev@56'
except:
    print('production copy mysql server failed connection')