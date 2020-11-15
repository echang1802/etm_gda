
import pandas as pd
from random import random
from datetime import datetime, timedelta

class wallet:

    """
    Tiene todo los movimientos de monedas de un usuario y calcula estadisticas
    sobre los consumos del mismo.
    """

    def __init__(self):
        self.data = pd.DataFrame()
        self.balance = 0
        self.index = 0

    def add(self, amount, datetime):
        self.balance += amount
        self.data = self.data.append(pd.DataFrame({
            "date" : datetime.date(),
            "earned" : amount,
            "spent" : 0,
            "balance" : self.balance
        }, index = { self.index }))
        self.index += 1

    def subtract(self, amount, datetime):
        self.balance -= amount
        self.data = self.data.append(pd.DataFrame({
            "date" : datetime.date(),
            "earned" : 0,
            "spent" : amount,
            "balance" : self.balance
        }, index = { self.index }))
        self.index += 1

    def get_days_between(self):
        datetime_shifted = self.data.loc[self.data["spent"] > 0, "date"].shift()
        days_between = self.data.loc[self.data["spent"] > 0, "date"] - datetime_shifted
        return days_between.dropna().apply(lambda x: x.days)

    def get_spent(self):
        return self.data.loc[self.data["spent"] > 0, "spent"]

    def get_earned(self):
        return self.data.loc[self.data["earned"] > 0, "earned"]

    def get_spent_average(self):
        data = self.data.loc[self.data["spent"] > 0]
        data = data[["date","spent"]].groupby("date").sum()
        return data.mean()

    def get_days_between_spents(self):
        data = self.data.loc[self.data["spent"] > 0, "date"].drop_duplicates()
        data["next_date"] = data["date"].shift()
        data["dif_dates"] = data["next_date"] - data["date"]
        return data["dif_dates"].dropna().apply(lambda x: x.days()).mean()


class distribution:

    """
    Calcula la función de disitribución acumulada empírica sobre los datos y luego,
    usando la función inversa genera muestras con la misma distribución.
    """

    def __init__(self, values):
        values = values.sort_values().reset_index(drop = True)
        self.values = [values[0]]
        self.dist = [0]
        n = 1 / values.size
        for value in values.unique():
            count = sum(values == value)
            self.values.append(value)
            self.dist.append(count * n)

        self.values.append(value + 1 if type(value) == int or type(value) == float else value)
        self.dist.append(n)

        self.values = pd.Series(self.values)
        self.dist = pd.Series(self.dist).cumsum()

    def get(self, n):
        values = []
        for i in range(n):
            aux = random()
            values.append(self.values[min(self.dist.index[self.dist >= aux])])
        return values

class user:

    """
    Tiene todo la información de un usuario, permitiendo asi, obtener estadísticas
    y realizar simulaciones.
    """

    def __init__(self, user_id, user_creation_time):
        self.id = user_id
        self.created = user_creation_time
        self.transactions = pd.DataFrame()
        self.sinks = {
            "sink_1" : 0,
            "sink_2" : 0,
            "sink_3" : 0,
            "sink_4" : 0,
            "sink_5" : 0,
            "sink_6" : 0,
            "sink_7" : 0
        }
        self.wallet = wallet()
        self.errors = {
            "invalid" : 0,
            "inconsistent" : 0
        }

    def add_transaction(self, transaction):
        if transaction.event_time < self.created or transaction.coins_balance < 0 or transaction.amount_spent <= 0:
            self.errors["invalid"] += 1
            return
        coin_gap = self.wallet.balance - transaction.amount_spent
        if transaction.coins_balance < coin_gap:
            self.errors["inconsistent"] += 1
            return

        if transaction.coins_balance > coin_gap:
            self.wallet.add(transaction.coins_balance - coin_gap, transaction.event_time)

        self.wallet.subtract(transaction.amount_spent, transaction.event_time)
        self.transactions = self.transactions.append(pd.DataFrame({
            "platform" : transaction.platform,
            "sink" : transaction.sink_channel,
            "spent" : transaction.amount_spent
        }, index = { transaction.event_time }))
        self.sinks[transaction.sink_channel] += transaction.amount_spent
        self.last_transaction = transaction.event_time

    def get_info(self):
        return pd.DataFrame({
            "spent" : self.wallet.get_spent().sum(),
            "earn" : self.waller.get_earned().sum(),
            "spent_average" : self.wallet.get_spent_average(),
            "days_between_spents" : self.wallet.get_days_between_spents(),
            "favorite_platform" : self.transactions["platform"].mode()
        }, index = { self.id })

    def validate(self):
        return self.transactions.shape[0] > 0 and \
            self.wallet.get_days_between().size > 0 and \
            self.wallet.get_spent().size > 0

    def simulate(self, date = None, days = 10):
        if date is None:
            date = self.last_transaction

        daysDistribution = distribution(self.wallet.get_days_between())
        spentDistribution = distribution(self.wallet.get_spent())
        sinkDistribution = distribution(self.transactions["sink"])
        platformDistribution = distribution(self.transactions["platform"])

        self.simulated_transactions = pd.DataFrame()
        endDate = date + timedelta(days = days)

        while True:
            date += timedelta(days = int(daysDistribution.get(1)[0]))
            if date > endDate:
                break
            self.simulated_transactions = self.simulated_transactions.append(pd.DataFrame({
                "platform" : platformDistribution.get(1)[0],
                "sink" : sinkDistribution.get(1)[0],
                "spent" : spentDistribution.get(1)[0]
            }, index = { date }))

    def get_transactions(self):
        data = self.transactions.copy().reset_index(drop = False)
        data["date"] = data["index"].apply(lambda x: x.date())
        data.drop(columns = "index", inplace = True)
        data["data_type"] = "real"
        return data

    def get_simulated_transactions(self):
        data = self.simulated_transactions.copy().reset_index(drop = False)
        data.rename(columns = {"index": "date"})
        data["data_type"] = "simulated"
        return data

class process:

    """
    Orquesta todo el procesamiento manteniendo limpio el ambiente de trabajo.
    """

    def __init__(self, inputDirectory, outputDirectory):
        from pathlib import Path

        self.inputDirectory = inputDirectory
        self.outputDirectory = outputDirectory

        data = pd.read_csv(self.inputDirectory)

        data["user_creation_time"] = data["user_creation_time"].apply(lambda x:  datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ"))
        data["event_time"] = data["event_time"].apply(lambda x:  datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ"))

        self.users = {}
        self.errors = {
            "invalid" : 0,
            "inconsistent" : 0
        }
        dataUsers = data["user_id"].unique()
        for u in dataUsers:
            auxData = data.loc[data["user_id"] == u].reset_index(drop = True)
            auxData.sort_values(by = "event_time", inplace = True)
            self.users[u] = user(u, auxData.at[0, "user_creation_time"])
            auxData.apply(self.users[u].add_transaction, axis = 1)
            self.errors["invalid"] += self.users[u].errors["invalid"]
            self.errors["inconsistent"] += self.users[u].errors["inconsistent"]
        print(self.errors)

        Path(self.outputDirectory).mkdir(exist_ok = True)

    def generate_user_info(self):
        users = pd.DataFrame()
        for u in self.users.values():
            users.append(u.get_info())

        users.to_csv(self.outputDirectory + "/users_info.csv", index = False)

    def simulate(self, days):
        data = pd.DataFrame()
        for u in self.users.values():
            if not u.validate():
                continue
            userData = u.get_transactions()
            u.simulate(days = days)
            userData = userData.append(u.get_simulated_transactions())
            userData["user_id"] = u.id
            data = data.append(userData)

        data.reset_index(drop = True, inplace = True)
        data.to_csv(self.outputDirectory + "/simulated_data.csv", index = False)
