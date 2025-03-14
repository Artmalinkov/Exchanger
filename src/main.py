'''
Основной функционал проекта согласно README
'''


def get_start():
    '''
    Начало работы, подключение.
    :return: об
    '''

    cursor_for_analytics = get_cursor('for_analytics')
    cursor_cerber = get_cursor('cerber')
    cursor_lemonchik = get_cursor('lemonchik')
    cursor_tochka = get_cursor('tochka')
    cursor_tmp = get_cursor('tmp')
    return cursor_for_analytics, cursor_cerber, cursor_lemonchik, cursor_tochka, cursor_tmp


cursor_for_analytics = get_cursor('for_analytics')
cursor_cerber = get_cursor('cerber')
cursor_lemonchik = get_cursor('lemonchik')
cursor_tochka = get_cursor('tochka')
cursor_tmp = get_cursor('tmp')

def main():
    # Получение объектов курсоров
    cursor_for_analytics, cursor_cerber, cursor_lemonchik, cursor_tochka, cursor_tmp = get_start()



if __name__ == '__main__':
    main()
    pass
