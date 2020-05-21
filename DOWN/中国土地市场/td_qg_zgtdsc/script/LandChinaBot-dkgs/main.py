from landchina import LandChinaBot

all_city = [{'city_code': '11', 'city_name': '北京市'}, {'city_code': '12', 'city_name': '天津市'},
            {'city_code': '13', 'city_name': '河北省'}, {'city_code': '14', 'city_name': '山西省'},
            {'city_code': '15', 'city_name': '内蒙古'}, {'city_code': '21', 'city_name': '辽宁省'},
            {'city_code': '22', 'city_name': '吉林省'}, {'city_code': '23', 'city_name': '黑龙江省'},
            {'city_code': '31', 'city_name': '上海市'}, {'city_code': '32', 'city_name': '江苏省'},
            {'city_code': '33', 'city_name': '浙江省'}, {'city_code': '34', 'city_name': '安徽省'},
            {'city_code': '35', 'city_name': '福建省'}, {'city_code': '36', 'city_name': '江西省'},
            {'city_code': '37', 'city_name': '山东省'}, {'city_code': '41', 'city_name': '河南省'},
            {'city_code': '42', 'city_name': '湖北省'}, {'city_code': '43', 'city_name': '湖南省'},
            {'city_code': '44', 'city_name': '广东省'}, {'city_code': '45', 'city_name': '广西壮族'},
            {'city_code': '46', 'city_name': '海南省'}, {'city_code': '50', 'city_name': '重庆市'},
            {'city_code': '51', 'city_name': '四川省'}, {'city_code': '52', 'city_name': '贵州省'},
            {'city_code': '53', 'city_name': '云南省'}, {'city_code': '54', 'city_name': '西藏'},
            {'city_code': '61', 'city_name': '陕西省'}, {'city_code': '62', 'city_name': '甘肃省'},
            {'city_code': '63', 'city_name': '青海省'}, {'city_code': '64', 'city_name': '宁夏回族'},
            {'city_code': '65', 'city_name': '新疆维吾尔'}, {'city_code': '66', 'city_name': '新疆建设兵团'}]


def main():
    for city in all_city:
        bot = LandChinaBot(city['city_code'], city['city_name'])
        bot.main()


if __name__ == '__main__':
    main()
