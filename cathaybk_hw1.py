import pandas as pd

def transTotalFloors(row):
    FloorStr=""
    digit_dic={"一" : 1, "二" : 2, "三" : 3, "四" : 4, "五" : 5, "六" : 6, "七" : 7, "八" : 8, "九" : 9, "十" : 10}
    if pd.notnull(row["總樓層數"]):
        FloorStr = row["總樓層數"][:-1]
        if len(FloorStr) == 1:
            row["TotalFloor"] = digit_dic[FloorStr]
        elif len(FloorStr) == 2:
            if FloorStr[0] == "十":
                row["TotalFloor"] = 10 + digit_dic[FloorStr[-1]]
            else:
                row["TotalFloor"] = digit_dic[FloorStr[0]] * 10
        elif len(FloorStr) == 3:
            row["TotalFloor"] = digit_dic[FloorStr[0]] * 10 + digit_dic[FloorStr[-1]]
    return row

def transTotalParking(row):
    totalparking=0
    if pd.notnull(row["交易筆棟數"]):
        row["TotalParking"] = int(row["交易筆棟數"][row["交易筆棟數"].find("位")+1:])
    
    return row


df_a = pd.read_csv("C:/Users/ACER/Desktop/Interview/國泰/內政部不動產(雙北_桃_中_高)/a_lvr_land_a.csv")
df_b = pd.read_csv("C:/Users/ACER/Desktop/Interview/國泰/內政部不動產(雙北_桃_中_高)/b_lvr_land_a.csv")
df_e = pd.read_csv("C:/Users/ACER/Desktop/Interview/國泰/內政部不動產(雙北_桃_中_高)/e_lvr_land_a.csv")
df_f = pd.read_csv("C:/Users/ACER/Desktop/Interview/國泰/內政部不動產(雙北_桃_中_高)/f_lvr_land_a.csv")
df_h = pd.read_csv("C:/Users/ACER/Desktop/Interview/國泰/內政部不動產(雙北_桃_中_高)/h_lvr_land_a.csv")

# 第一列為Title英文，故將之去掉
df_a.drop([0], axis=0, inplace=True)
df_b.drop([0], axis=0, inplace=True)
df_e.drop([0], axis=0, inplace=True)
df_f.drop([0], axis=0, inplace=True)
df_h.drop([0], axis=0, inplace=True)

# df_a 700
# df_b 1169
# df_e 1047
# df_f 1404
# df_h 1266
# print(len(df_a), len(df_b), len(df_e), len(df_f), len(df_h))

df_all = pd.concat([df_a, df_b, df_e, df_f, df_h])

# df_all 5586
# print(len(df_all))

# 新增一欄 TotalFloor
df_all["TotalFloor"] = 0
df_all = df_all.apply(transTotalFloors, axis = 1)

filter_a = df_all[ (df_all["主要用途"] == "住家用") & (df_all["建物型態"] == "住宅大樓(11層含以上有電梯)") & (df_all["TotalFloor"] >= 13) ]
filter_a.to_csv("filter_a.csv", encoding='utf_8_sig')

df_all["TotalParking"] = 0
df_all = df_all.apply(transTotalParking, axis = 1)

df_all[["總價元", "車位總價元"]] = df_all[["總價元", "車位總價元"]].astype(int)

b_dict = {
    "總件數" : len(df_all),
    "總車位數" : df_all["TotalParking"].sum(),
    "平均總價元" : df_all["總價元"].mean(),
    "平均車位總價元" : df_all["車位總價元"].mean()
}

filter_b = pd.DataFrame(b_dict,index=[0])
filter_b.to_csv("filter_b.csv", encoding='utf_8_sig')