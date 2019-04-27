from tkinter import *
from tkinter import ttk

def get_selection():
    items = [languages[int(item)] for item in  p1_IncludeLanguage.curselection()]
    print(items)

main = Tk()
main.title('Test GUI')
main.geometry('700x500')

nb = ttk.Notebook(main)
nb.grid(row=1, column=0, columnspan=50, rowspan=49, sticky='NESW')

page1 = ttk.Frame(nb)
nb.add(page1, text='Search Terms')

p1_IncludeLanguage = Listbox(page1)
p1_IncludeLanguage.configure(selectmode=MULTIPLE, width=9, height=5)
p1_IncludeLanguage.grid(row=0, column=0)

btnGet = Button(page1,text="Get Selection",command=get_selection)
btnGet.grid()

languages = ["English", "German", "French", "Italian", "Portuguese"]

for lang in languages:
    p1_IncludeLanguage.insert(END, lang)



main.mainloop()
