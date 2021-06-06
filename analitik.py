import os
import csv
import threading as th


class Treader(th.Thread):
    bd = {}
    itogi_bd = {}
    sort_bd = ()

    def __init__(self, file, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.way = file
        self.files_list = [os.path.join(root, fname) for root, sub, files in os.walk(self.way) for fname in files]

    def run(self):
        with open(self.way) as file:
            reader = csv.DictReader(file, 'NTPQ')
            for line in reader:
                if line['N'] != 'SECID' and line['P'] != 'sltnvv':
                    if line['N'] not in self.bd.keys():
                        self.bd[line['N']] = [float(line['P'])]
                    else:
                        self.bd[line['N']].append(float(line['P']))

    def rezult(self):
        for i in self.bd.keys():
            maxi = max(self.bd[i])
            mini = min(self.bd[i])
            sr = (maxi + mini) / 2
            volatility = ((maxi - mini) / sr) * 100
            self.itogi_bd[i] = volatility
        self.sort_bd = list(self.itogi_bd.items())
        self.sort_bd.sort(key=lambda i: i[1])
        maxi_sp = [self.sort_bd[-1], self.sort_bd[-2], self.sort_bd[-3]]
        mini_sp = []
        null_sp = []
        n = 0
        while len(mini_sp) != 3:
            d = self.sort_bd[n]
            if d[1] == 0.0:
                null_sp.append(d[0])
            else:
                mini_sp.append(d)
            n += 1
        null_sp.sort()
        return (f'''Максимальная волатильность:
        {maxi_sp[0][0]} - {round(maxi_sp[0][1], 2)} %
        {maxi_sp[1][0]} - {round(maxi_sp[1][1], 2)} %
        {maxi_sp[2][0]} - {round(maxi_sp[2][1], 2)} %
Минимальная волатильность:
        {mini_sp[2][0]} - {round(mini_sp[2][1], 2)} %
        {mini_sp[1][0]} - {round(mini_sp[1][1], 2)} %
        {mini_sp[0][0]} - {round(mini_sp[0][1], 2)} %
Нулевая волатильность:
        {', '.join(null_sp)}''')


way = r"/home/sltnvv/Загрузки/trades"
spisok_file = [os.path.join(root, fname) for root, sub, files in os.walk(way) for fname in files]
treaders = [Treader(file=a) for a in spisok_file]
for i in treaders:
    i.start()
for i in treaders:
    i.join()

itog = Treader('pass')
print(itog.rezult())
