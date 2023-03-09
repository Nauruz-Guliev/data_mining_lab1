import psycopg2


def read_from_db():
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="nauruz0304",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="postgres")
        cursor = connection.cursor()
        postgreSQL_select_Query = "select * from вДудь"

        cursor.execute(postgreSQL_select_Query)
        channel_data = cursor.fetchall()
        data = []
        for row in channel_data:
            res = dict(title=row.__getitem__(1),
                       ref_domain_url=row.__getitem__(2),
                       views=row.__getitem__(5),
                       description_length=row.__getitem__(6),
                       likes=row.__getitem__(7),
                       comments=row.__getitem__(8),
                       duration=row.__getitem__(9),
                       subscriber_count=row.__getitem__(10))
            data.append(res)
        return data
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)


raw_data = read_from_db()

print(raw_data)


def calculate_CT(data):
    return float(data['description_length']) + float(data['duration'])


def calculate_ER(data):
    return float(data['views']) / float(data['subscriber_count']) * 100


def calculate_CP(data, cps=0.00045):
    reach1 = float(data['views'])
    reach2 = (float(data['likes']) + 1) + ((reach1) - float(float((data['likes']))) + 1) * (
                (float((data['likes'])) + 1) / reach1)
    ke = 0
    value = int(float(data['duration']))
    if value in range(0, 10):
        ke = 0.95
    if value in range(11, 30):
        ke = 0.85
    if value in range(31, 60):
        ke = 0.75
    if value in range(61, 120):
        ke = 0.70
    if value in range(121, 180):
        ke = 0.65
    if value in range(181, 240):
        ke = 0.62
    if value in range(241, 300):
        ke = 0.60
    if value in range(241, 1200):
        ke = 0.31
    if value in range(1201, 7200):
        ke = 0.20
    if value in range(7201, 19800):
        ke = 0.2
    CP = reach1 * 5 * cps + reach2 * ((float(data['duration']) + float(data['comments'])) - 5) * ke*cps
    return CP


def calculate_average_values(data):
    length = len(data)
    CT_AVERAGE = 0
    ER_AVERAGE = 0
    CP_AVERAGE = 0
    CP_MIN_AVERAGE = 0
    for video in data:
        CT_AVERAGE += calculate_CT(video)
        ER_AVERAGE += calculate_ER(video)
        CP_AVERAGE += calculate_CP(video)
    CT_AVERAGE = CT_AVERAGE/length
    ER_AVERAGE = ER_AVERAGE/length
    CP_AVERAGE = CP_AVERAGE/length
    CP_MIN_AVERAGE = CP_AVERAGE/CT_AVERAGE
    print("ER " + str(ER_AVERAGE))
    print("CT " + str(CT_AVERAGE))
    print("CP " + str(CP_AVERAGE))
    print("CP_MIN " + str(CP_MIN_AVERAGE))



calculate_average_values(raw_data)