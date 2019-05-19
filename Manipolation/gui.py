#from tkinter import Tk, Label, Button, Listbox, MULTIPLE, END
import tkinter

class my_gui:
    def __init__(self, master, ds_names):
        self.master = master
        self.ds_names = ds_names
        master.title("A simple GUI")

        self.label = tkinter.Label(master, text="This is our first GUI!")
        self.label.pack()

        self.ds_list_box = tkinter.Listbox(master)
        self.ds_list_box.configure(selectmode=tkinter.MULTIPLE, width=50, height=20)
        self.ds_list_box.pack(side="left",fill="both", expand=True)

        self.ds_button = tkinter.Button(master,text="Get Selection",command= self.get_selection)
        self.ds_button.pack()
        for name in ds_names:
            self.ds_list_box.insert(tkinter.END, name)

        self.close_button = tkinter.Button(master, text="Close", command=master.quit)
        self.close_button.pack()

    def greet(self):
        print("Greetings!")

    def get_selection(self):
        self.selected_options = [self.ds_names[int(item)] for item in  self.ds_list_box.curselection()]


if __name__ == "__main__":
    root = tkinter.Tk()
    ds_names = ["Ciao", "ciao2", "ciao3"]
    menu = my_gui(root, ds_names)
    root.mainloop()
    print menu.selected_options
