import os
from collections import defaultdict
# שיניתי את פורמט הקובץ logs.txt.xlsx ל logs1.txt כי ביקשו קלט של קובץ טקסט
# שלב 1: פיצול הקובץ logs1.txt לקבצים קטנים יותר
def split_log_file(file_path, lines_per_file=100000):
    file_index = 1
    lines = []
    count = 0

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            lines.append(line.strip())
            count += 1

            if count >= lines_per_file:
                with open(f"logs_part_{file_index}.txt", "w", encoding="utf-8") as part_file:
                    part_file.write("\n".join(lines))
                print(f"נשמר קובץ: logs_part_{file_index}.txt")
                file_index += 1
                lines = []
                count = 0

    if lines:
        with open(f"logs_part_{file_index}.txt", "w", encoding="utf-8") as part_file:
            part_file.write("\n".join(lines))
        print(f"נשמר קובץ: logs_part_{file_index}.txt")

# שלב 2: ספירת קודי שגיאה בקובץ אחד
def count_errors(file_path):
    errors = defaultdict(int)
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            code = line.strip()
            if code:
                errors[code] += 1
    return errors

# שלב 3: מיזוג כל הספירות למילון אחד
def merge_counts(files):
    all_counts = defaultdict(int)
    for file in files:
        part = count_errors(file)
        for code, num in part.items():
            all_counts[code] += num
    return all_counts

# שלב 4: מציאת N הקודים הכי נפוצים
def get_top_n(counts, n):
    sorted_list = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    return sorted_list[:n]

# מחיקת קבצי עזר
def clean_parts():
    removed = 0
    for name in os.listdir():
        if name.startswith("logs_part_") and name.endswith(".txt"):
            os.remove(name)
            removed += 1
    print(f"נמחקו {removed} קבצים זמניים.")

# הפעלה ראשית
def main():
    input_file = "logs1.txt"
    lines_per_file = 100_000
    N = 5  # מספר קודי השגיאות הכי נפוצות שרוצים לראות

    if not os.path.exists(input_file):
        print("לא נמצא קובץ logs1.txt")
        return

    split_log_file(input_file, lines_per_file)

    part_files = [f for f in os.listdir() if f.startswith("logs_part_") and f.endswith(".txt")]
    total = merge_counts(part_files)

    if N > len(total):
        print(f"יש פחות שגיאות , יוחזרו רק {len(total)} הקיימים.")
        N = len(total)

    top_n = get_top_n(total, N)

    print(f"\n{N} קודי השגיאה הכי נפוצים:")
    for code, count in top_n:
        print(f"{code}: {count}")

    clean_parts()

if __name__ == "__main__":
    main()

# =================
# שלב 5: סיבוכיות
# =================
#
# L - מספר שורות בקובץ
# U - מספר שגיאות שונים
# N - כמה שגיאות רוצים
#
# זמן ריצה:
# - קריאה ופיצול - O(L)
# - ספירה בכל קובץ - O(L)
# - מיזוג לאחד - O(U)
# - מיון למציאת N הנפוצים - O(U log U)
# סה"כ בערך O(L + U log U)
#
# זיכרון:
# - מילון לכל השגיאות -> O(U)
# - רשימת תוצאה -> O(N)
# הקבצים נשמרים בדיסק, ולא משפיעים על הזיכרון
#  סה"כ: O(U + N)
