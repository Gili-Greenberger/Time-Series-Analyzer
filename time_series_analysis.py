import pandas as pd
import os

# השתמשתי בספריית pandas לצורך קריאה, עיבוד וניתוח סדרות זמן בקבצי (CSV ו-Parquet).
# שיניתי את פורמט הקובץ time_series.xlsx ל time_series.csv כי ביקשו קלט של קובץ csv
# חלק 1 א - בדיקת תקינות של קובץ time_series
def validate_time_series(df):
    df.columns = df.columns.str.strip().str.lower()

    try:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='raise')
    except Exception as e:
        raise ValueError("לא ניתן להמיר את העמודה 'timestamp' לתאריך. שגיאה: " + str(e))

    if df.duplicated().any():
        print("יש שורות כפולות בקובץ")

    # קביעה חכמה של עמודת הערכים
    if 'value' not in df.columns:
        if 'mean_value' in df.columns:
            df['value'] = df['mean_value'] #-- בשביל שני הקבצים .csv ו .parquet
            print("העמודה 'value' לא נמצאה – השתמשתי ב-'mean_value' במקום.")
        else:
            raise ValueError("לא נמצאה עמודה מתאימה לערכים מספריים ('value' או 'mean_value')")

    df['value'] = pd.to_numeric(df['value'], errors='coerce')

    if df['value'].isnull().any():
        print("יש ערכים לא מספריים בעמודת value")

    if df.isnull().any().any():
        print("חסרים ערכים בטבלה")

    return df


#  חישוב ממוצעים לפי שעות
def hourly_average(df):
    df['hour'] = df['timestamp'].dt.floor('h')
    avg_df = df.groupby('hour', as_index=False)['value'].mean()
    avg_df.rename(columns={'hour': 'זמן התחלה', 'value': 'ממוצע'}, inplace=True)
    return avg_df

# קריאת הקובץ - csv או parquet
def read_time_series(file_path):
    """
    חלק 4
    קריאת קובץ time_series מסוג CSV או Parquet.

    יתרונות פורמט Parquet:
    1. אחסון בעמודות (Columnar) מאפשר דחיסה טובה וקריאה מהירה.
    2. ניתן לקרוא רק עמודות ספצפיות – לא חייבים לקרוא את כל הנתונים.
    3. פורמט נפוץ בעולמות ה Big Data .
    """
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith('.parquet'):
        return pd.read_parquet(file_path)
    else:
        raise ValueError("פורמט לא נתמך. יש להשתמש בקובץ .csv או .parquet בלבד.")


#  קוד לפי חלק 1 ב
# def main():
#     file_path = "time_series.csv"
#     if not os.path.exists(file_path):
#         print(f"לא נמצא קובץ בשם: {file_path}")
#         return
#     try:
#         df = read_time_series(file_path)
#         df = validate_time_series(df)
#     except Exception as e:
#         print("שגיאה בטעינת או בעיבוד הקובץ: " + str(e))
#         return
#     hourly_avg = hourly_average(df)
#     print(hourly_avg.to_string(index=False))

# חלק 2 חילוק הנתונים לפי ימים
def split_by_day(df):
    df['day'] = df['timestamp'].dt.floor('d')
    return [group.copy() for _, group in df.groupby('day')]

# פונקציה ראשית לעיבוד יומי
def main_daily():
    #file_path = "time_series.csv" #-- לפורמט .csv
    file_path = "time_series (4).parquet"
    if not os.path.exists(file_path):
        print(f"הקובץ {file_path} לא נמצא.")
        return

    try:
        df = read_time_series(file_path)
        df = validate_time_series(df)
    except Exception as e:
        print("שגיאה בקריאת או בעיבוד הקובץ: " + str(e))
        return

    daily_data = split_by_day(df)
    daily_results = []

    for day_df in daily_data:
        avg_df = hourly_average(day_df)
        daily_results.append(avg_df)

    final_result = pd.concat(daily_results, ignore_index=True)
    final_result = final_result.sort_values('זמן התחלה').reset_index(drop=True)

    print(final_result.to_string(index=False))

    final_result.to_csv("final_hourly_average.csv", index=False)
    print("\nהקובץ final_hourly_average.csv נוצר ונשמר בהצלחה.")

if __name__ == "__main__":
    # main()  #
    main_daily()


"""
חלק 3:
פתרון לעדכון ממוצעים שעתיים בזמן אמת כאשר הנתונים מגיעים כזרימה (stream)

1. חלונות זמן (Windowing):
   - אגדיר חלון זמן שבו נאספים הנתונים לצורך חישוב הממוצע.

2. עיבוד מצב (Stateful Processing):
   - אשמור משתנים מצטברים (כמו סכום הערכים ומספר החזרות) עבור כל חלון.
   - כל נתון חדש מעדכן את הערכים המצטברים, וכך ניתן לעדכן את הממוצע באופן מצטבר.

3. טיפול בנתונים מאוחרים (Late Arriving Data):
   - אגדיר מנגנונים (כגון watermarking) שמאפשרים לקבוע עד מתי נתונים עבור חלון מסוים יכולים להתעדכן.
   - כך ניתן לטפל במקרים שבהם נתונים מגיעים לאחר סגירת החלון.
"""
