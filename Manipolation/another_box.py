from tkinter import Tk, Label, Button, Listbox, MULTIPLE, END

class Gui:
    def __init__(self, master, ds_names):
        self.master = master
        self.ds_names = ds_names
        master.title("A simple GUI")

        self.label = Label(master, text="This is our first GUI!")
        self.label.pack()

        self.ds_list_box = Listbox(master)
        self.ds_list_box.configure(selectmode=MULTIPLE, width=9, height=5)
        self.ds_list_box.pack()

        self.ds_button = Button(master,text="Get Selection",command= self.get_selection)
        self.ds_button.pack()
        for name in ds_names:
            self.ds_list_box.insert(END, name)

        self.close_button = Button(master, text="Close", command=master.quit)
        self.close_button.pack()

    def greet(self):
        print("Greetings!")

    def get_selection(self):
        self.selected_options = [ds_names[int(item)] for item in  self.ds_list_box.curselection()]


if __name__ == "__main__":
    root = Tk()
    ds_names = ["Ciao", "ciao2", "ciao3"]
    my_gui = Gui(root, ds_names)
    root.mainloop()
    print my_gui.selected_options
