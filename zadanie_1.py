# Biblioteka — wersja podstawowa
# Programowanie strukturalne: funkcje, pętle, instrukcje warunkowe, listy i słowniki

books = [
    {"title": "Lalka", "author": "Bolesław Prus", "copies": 3},
    {"title": "Pan Tadeusz", "author": "Adam Mickiewicz", "copies": 2},
    {"title": "Quo Vadis", "author": "Henryk Sienkiewicz", "copies": 4},
    {"title": "Ferdydurke", "author": "Witold Gombrowicz", "copies": 1},
    {"title": "Zbrodnia i kara", "author": "Fiodor Dostojewski", "copies": 2},
]

users = [
    {"login": "jan", "password": "1234", "role": "czytelnik", "borrowed": []},
    {"login": "anna", "password": "abcd", "role": "czytelnik", "borrowed": []},
    {"login": "piotr", "password": "pass", "role": "czytelnik", "borrowed": []},
]


def login_user():
    attempts = 0

    while attempts < 3:
        login = input("Login: ")
        password = input("Hasło: ")

        for user in users:
            if user["login"] == login and user["password"] == password:
                print(f"\nZalogowano jako: {user['login']}\n")
                return user

        attempts += 1
        print(f"Niepoprawny login lub hasło. Pozostało prób: {3 - attempts}")

    print("Przekroczono limit prób logowania. Program zakończony.")
    return None


def show_catalog():
    print("\nKATALOG KSIĄŻEK")
    print("-" * 40)

    for book in books:
        print(f"Tytuł: {book['title']}")
        print(f"Autor: {book['author']}")
        print(f"Dostępne sztuki: {book['copies']}")
        print("-" * 40)


def find_book_by_title(title):
    for book in books:
        if book["title"].lower() == title.lower():
            return book

    return None


def borrow_book(user):
    title = input("Podaj tytuł książki do wypożyczenia: ")
    book = find_book_by_title(title)

    if book is None:
        print("Nie znaleziono książki o podanym tytule.")
        return

    if book["copies"] <= 0:
        print("Brak dostępnych sztuk tej książki.")
        return

    book["copies"] -= 1
    user["borrowed"].append(book["title"])

    print(f"Wypożyczono książkę: {book['title']}")


def show_my_borrowed_books(user):
    print("\nMOJE WYPOŻYCZENIA")
    print("-" * 40)

    if len(user["borrowed"]) == 0:
        print("Nie masz aktualnie wypożyczonych książek.")
        return

    for index, title in enumerate(user["borrowed"], start=1):
        print(f"{index}. {title}")


def show_menu():
    print("\nMENU")
    print("1. Przeglądaj katalog")
    print("2. Wypożycz książkę")
    print("3. Moje wypożyczenia")
    print("4. Wyloguj")


def main():
    logged_user = login_user()

    if logged_user is None:
        return

    while True:
        show_menu()
        choice = input("Wybierz opcję: ")

        if choice == "1":
            show_catalog()
        elif choice == "2":
            borrow_book(logged_user)
        elif choice == "3":
            show_my_borrowed_books(logged_user)
        elif choice == "4":
            print("Wylogowano. Do widzenia!")
            break
        else:
            print("Niepoprawny wybór. Spróbuj ponownie.")


main()