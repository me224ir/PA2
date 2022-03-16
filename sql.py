
import csv
from attr import define
import mysql.connector as mc
import msvcrt
import socket

hostname = socket.gethostname()
localip = socket. gethostbyname(hostname)

con_sql=mc.connect(host="localhost", user="root", passwd = "root")

def createDB_cities():                                                            # Creates list for all elements for planets table

    cities = open(r'cities1.csv',encoding="cp1252", )              # Opens csv file and 'reads' it
    read_cities = csv.reader(cities)

    p_rows1 = []
    p_rows2 = []
    for row in read_cities:
            p_rows1.append(row)
    


    for row in p_rows1:                               # Labels all NULL values and turns string-integers to actual integers so it can be inserted into database
        p_row=[]
        for value in row:
            if value.isdigit() == True:
                value = int(value)
            p_row.append(value)
        p_rows2.append(p_row)
    return p_rows2


def createDB_universities():                                                                       # Creates list for all elements for species table

    universities = open(r'universities1.csv',encoding="cp1252")                         # Opens csv file and 'reads' it

            
        
    s_rows1 = []
    s_rows2 = []
    read_universities = csv.reader(universities)

    for row in read_universities:
        s_rows1.append(row)



    for row in s_rows1:                                                                       # Labels all NULL values and turns string-integers to actual integers so it can be inserted into database 
        s_row=[]
        for value in row:
            if value.isdigit() == True:
                value = int(value)
            s_row.append(value)
        s_rows2.append(s_row)

            

    return s_rows2

def createDB_lessor():
    lessor = open(r'lessor1.csv',encoding='cp1252')                         # Opens csv file and 'reads' it
    read_lessor = csv.reader(lessor)
    
    s_rows1 = []
    s_rows2 = []
    
    for row in read_lessor:
        s_rows1.append(row)


    for row in s_rows1:                                                                       # Labels all NULL values and turns string-integers to actual integers so it can be inserted into database 
        s_row=[]
        for value in row:
            if value.isdigit() == True:
                value = int(value)
            s_row.append(value)
        s_rows2.append(s_row)

            

    return s_rows2

def main_menu():                                                             # Shows and directs to all options
    print("You have entered the main menu")
    main_menu_value = int(input("Choose one of the following by inputting the corresponding number: \n 1. Check the different divisions in each of the covered regions' universities/colleges \n 2. Check the average amount of students per lessor and the average amount of apartments per lessor \n 3. Statistics per city \n 4. Check what lessor has the apartment with the most rooms in each city \n 5. Exit  \n"))
    null_lst = [0, "0"]
    if main_menu_value == 1:                                                 # Option 1
        print("The cities and divisions: ")
        db.execute("select c_region, group_concat(u_name, ': ', u_divisions, ' ') from cities inner join universities on u_city = c_name group by c_region")
        results = db.fetchall()
        for rawrow in results:
            print(rawrow[0] + ": " + rawrow[1] + "\n")
    
    elif main_menu_value == 2:                                                # Option 2
        search = "select (select sum(u_enrolled) from universities)/(select count(l_name) from lessor), avg(l_apartment_amount) from lessor"
        db.execute(search)
        results = db.fetchall()
        for rawrow in results:
            print(round(rawrow[0]), "students per lessor and", round(rawrow[1]), "apartments per lessor")

    elif main_menu_value == 3:                                                # Option 3
        city = str(input("Select one of the cities to look into by typing it's name: (all 'ö' needs to be written as 'oe', all 'ä' needs to be written as 'ae' and all 'å' need to be written as 'aa')"))
        view = "create view city as select u_city, u_name, u_enrolled, l_apartment_amount, c_region, c_population from universities inner join lessor on l_city = u_city inner join cities on c_name = u_city where u_city = '" + city + "' group by u_name"
        db.execute(view)
        city_value = int(input("Choose one of the following by inputting the corresponding number: \n 1. The percentage of students among population (if all students live in the city) \n 2. The name of all student-apartment lessors in the city and if they have queue \n 3. All universities in city and their amount of enrolled students \n- "))
        if city_value == 1:
            db.execute("select (sum(u_enrolled)/c_population)*100 from city")
        elif city_value ==2:
            db.execute("select u_city, l_name, l_queue from city inner join lessor on u_city = l_city group by l_name")
        else:
            db.execute("select u_name, u_enrolled from city")
        results = db.fetchall()
        db.execute("drop view city")
        for row in results:
            if city_value == 1:
                for element in row:
                    print(str(element) + "%")
            elif city_value == 2:
                print(row[0] + ":", row[1] + ", has queue?: " + row[2], end=", \n")
            else:
                print("University: " + row[0] + ", enrolled students: " + str(row[1]))

    elif main_menu_value == 4:
        db.execute("select l_city, l_name, max(l_max_rooms) from lessor group by l_city")
        results = db.fetchall()
        for rawrow in results:
            print(rawrow[0] +": " + rawrow[1] + " has the apartment(s) with the most rooms: " + str(rawrow[2]) + " rooms")



    elif main_menu_value == 5:                                               
        print("Exiting database")
        quit()                

    print("\nClick on any key to get back to the main menu")
    msvcrt.getch()
    main_menu()

    



db = con_sql.cursor()                                    
db.execute("show databases")
dblst=list(db.fetchall())
refinedlst = []
for dbs in dblst:
    dbs = str(dbs)
    dbs.replace("(","")
    dbs.replace("'","")
    dbs.replace(",","")
    dbs.replace(")","")
    refinedlst.append(dbs)
db.execute("SET FOREIGN_KEY_CHECKS=0")




DB_NAME="university_lessor_city"
db_check="('university_lessor_city',)"



s_rows = []

if db_check in refinedlst:                                                           # Checks if database exists, if it does
    db.execute("use university_lessor_city")                                                       # uses it, else it creates it
    print("You succesfully entered the database")
    menu_value = int(input("Choose one of the following by inputting the corresponding number: \n 1. Go to main menu \n 2. Exit \n 3. Delete database \n"))
    if menu_value == 2:                                                              # After starting to use database, you get into a menu with three options
        print("Exiting database")                                                    # Option 2, quit
        quit()
    if menu_value == 3:                                                              # Option 3, delete database (mainly for me to be able to test new things quickly)
        db.execute("drop database university_lessor_city")
        print("Dropping database ", DB_NAME)

else: 
    db.execute("create database {}".format(DB_NAME))                                 # Here it creates databse if it does not exist
    s_rows = createDB_cities()
    p_rows = createDB_universities()
    q_rows = createDB_lessor()
    print(p_rows)
    print(s_rows)
    print(q_rows)
    db.execute("use university_lessor_city")
    db.execute("create table universities (u_name varchar(75) not NULL, u_abbreviation varchar(10) not NULL primary key, u_divisions varchar(500), u_enrolled int, u_city varchar(75) not NULL)")
    db.execute("create table lessor (l_name varchar(75) not NULL primary key, l_city varchar(75) not NULL, l_apartment_amount int, l_min_rooms int, l_max_rooms int, l_queue varchar(10))")
    db.execute("create table cities (c_name varchar(75) not NULL primary key, c_region varchar(75), c_population int)")
    db.execute("alter table universities add constraint fk_cities1 foreign key (u_city) references cities (c_name)")
    db.execute("alter table lessor add constraint fk_cities2 foreign key (l_city) references cities (c_name)")
    p_execution = "insert into universities values (%s, %s, %s, %s, %s)"
    db.executemany(p_execution, p_rows)
    s_execution = "insert into cities values (%s, %s, %s)"
    db.executemany(s_execution, s_rows)
    q_execution = "insert into lessor values (%s, %s, %s, %s, %s, %s)"
    db.executemany(q_execution, q_rows)
    con_sql.commit()

    print("Database created")
    quit()


if menu_value == 1:                                                                   # If you pick option 1, main menu emerges
    main_menu()



    