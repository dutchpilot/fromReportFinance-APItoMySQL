import requests
from mysql.connector import connect, Error
import sys
from datetime import datetime
import config


headers = {'X-API-KEY': config.API_KEY}

# Connect to MySQL server
try:
    db_connection = connect(user=config.USER, password=config.PASSWORD, host=config.HOST, port=config.PORT, database=config.DATABASE, auth_plugin='mysql_native_password')
    #db_connection = connect(user='user', password='password', host='192.168.0.13', port='3306', database='mydb')
    cursor = db_connection.cursor()
except Error as e:
    print('ERROR: ' + str(e))
    sys.exit(0)


def insert_to_db(api_name, jsongroup_name, db_name):
    # show_table_query = "TRUNCATE " + db_name
    # cursor.execute(show_table_query)

    show_table_query = "DESCRIBE " + db_name
    cursor.execute(show_table_query)
    result = cursor.fetchall()
    db_fields = ""
    formatted_string_for_update_template = ""
    i = 1
    for row in result:
        db_fields = db_fields + row[0] + ","
        i += 1
        formatted_string_for_update_template = formatted_string_for_update_template + row[0] + "='{" + str(
            i - 3) + "}',"

    db_fields = db_fields[0:len(db_fields) - 1]
    print('\n' + db_name + ': '  + db_fields)
    list_fields = db_fields.split(',')


    req = requests.get(config.URL + api_name, headers=headers)

    if jsongroup_name == '':
        totalLineCount = len(req.json())
        limit = 100
    else:
        totalLineCount = req.json()['totalLineCount']
        limit = req.json()['limit']

    print('totalLineCount=' + str(totalLineCount) + ' limit=' + str(limit))
    current_offset = 0

    while current_offset < totalLineCount:
        params = 'offset='+str(current_offset)
        req = requests.get(config.URL + api_name, params=params, headers=headers)
        json_data = req.json()
        #print(json_data)

        if jsongroup_name == '':
            pure_json_data = json_data
        else:
            pure_json_data = json_data[jsongroup_name]

        inserted = 0
        updated = 0
        for item in pure_json_data:

            cursor.execute("SELECT COUNT(*) FROM " + db_name + " WHERE id={0}".format(item['id']))
            rowcount_byId = cursor.fetchone()[0]

            if rowcount_byId == 0:
                data = ''
                for item_list_fields in list_fields:
                    data = data + "'" + str(item[item_list_fields]).replace("'","|") + "',"
                data = data[0:len(data) - 1]
                #print(data)
                insert_clients_query = "INSERT INTO " + db_name + " ({0}) VALUES ({1})".format(db_fields, data)

                try:
                    cursor.execute(insert_clients_query)
                    db_connection.commit()
                except Error as e:
                    print('ERROR: ' + str(e))
                    sys.exit(0)

                inserted += 1

            elif rowcount_byId == 1:
                data = ''
                i = 0
                for item_list_fields in list_fields:
                    data = data + list_fields[i] + "='" + str(item[item_list_fields]).replace("'", "|") + "',"
                    i += 1
                data = data[0:len(data) - 1]

                #print(data)
                update_clients_query = "UPDATE " + db_name + " SET {0} WHERE id='{1}'".format(data, item['id'])

                try:
                    cursor.execute(update_clients_query)
                    db_connection.commit()
                except Error as e:
                    print('ERROR: ' + str(e))
                    sys.exit(0)

                updated += 1

            # else:
            #     print('ERROR: key field id=' + str(item['id']) + ' is not unique')

        print('offset=' + str(current_offset) + ': ' + str(updated) + ' updated records; ' + str(inserted) + ' inserted records')

        current_offset += limit

start_time = datetime.now().strftime("%H:%M:%S")

insert_to_db('Streams', '', 'finansist_streams')
insert_to_db('Payments', 'listPayment', 'finansist_payments')
insert_to_db('Contragents', 'listContragent', 'finansist_contragents')
insert_to_db('Invoices'  , 'listInvoice'  , 'finansist_invoices')
insert_to_db('Documents', 'listDocument', 'finansist_documents')
insert_to_db('Accounts', 'listAccount', 'finansist_accounts')
insert_to_db('Projects', 'listProject', 'finansist_projects')
insert_to_db('Organisations', 'listOrganisation', 'finansist_organisations')
insert_to_db('Items'  , 'ListItem'  , 'finansist_items')

finish_time = datetime.now().strftime("%H:%M:%S")

print(str(start_time) + '-' + str(finish_time))

db_connection.close()
