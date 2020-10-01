from tkinter import ttk
import tkinter as tk

from functools import partial

import os
import sys

import slack_export

root = tk.Tk()

WindowWidth = 300
WindowHeight = 100

def demo():

    s = ttk.Style()
    s.configure("Grey.TFrame", background="grey")

    root.title("Slack Private Messages Exporter")
    root.geometry(str(WindowWidth)+"x"+str(WindowHeight))  # You want the size of the app to be 500x500
    root.resizable(0, 0)  # Don't allow resizing in the x or y direction

    f1 = ttk.LabelFrame(root, text="Please enter Slack OAuth Access Token: ", width=WindowWidth-5, height=WindowHeight-5)
    f1.grid_propagate(0)
    f1.grid_columnconfigure((0, 1, 2), weight=1)
    f1.grid(row=0, column=0)
    f1.place(x=2, y=2)

    entry1 = tk.Entry(f1, width=WindowWidth-20)
    entry1.grid(row=1, column=1, pady=5)

    Button1 = tk.Button(f1, text="Export", command=partial(RunSlackExport, entry1))
    Button1.grid(row=3, column=1, pady=10)

def RunSlackExport (entry1):
    slack_export.AllPrivateMessagesWrapper(entry1.get())

if __name__ == "__main__":
    demo()
    root.mainloop()
