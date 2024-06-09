import sqlite3
import random
def create_and_populate_db(db_name, num_rows):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS mydata
                (id INTEGER PRIMARY KEY, ho TEXT, ten_dem TEXT, ten TEXT)''')

    for i in range(num_rows):
        c.execute("INSERT INTO mydata (ho, ten_dem, ten) VALUES (?, ?, ?)", ('Nguyen_'+str(random.randint(0, 10)), 'Huu_'+str(random.randint(0, 10)), 'Thang_'+str(random.randint(0, 10))))

    conn.commit()
    conn.close()

create_and_populate_db('data_cua_thang.db', 1000000)
