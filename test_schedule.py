import datetime


def get_test_schedule():
    now = datetime.datetime.now()

    start = now + datetime.timedelta(minutes=1)
    end = start + datetime.timedelta(minutes=2)
    date_str = start.strftime('%d.%m.%Y')
    time_str = f"{start.strftime('%H:%M')} - {end.strftime('%H:%M')}"

    class1 = {
        "DATE_Z": date_str,
        "TIME_Z": time_str,
        "DISCIP": "Иностранный язык",
        "KOW": "Практические занятия (англ)",
        "AUD": "9-328",
        "PREP": "Филимонова Ольга Викторовна",
        "GROUPS": [{"GROUP_P": "111111", "PRIM": ""}],
        "CLASS": "practice"
    }

    class2 = {
        "DATE_Z": date_str,
        "TIME_Z": time_str,
        "DISCIP": "Программирование",
        "KOW": "Лабораторные занятия",
        "AUD": "Гл.-418-д",
        "PREP": "Креслинь Мария Владимировна",
        "GROUPS": [{"GROUP_P": "111111", "PRIM": ""}],
        "CLASS": "lab"
    }

    return [class1, class2]
