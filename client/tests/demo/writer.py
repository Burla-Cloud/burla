import pyautogui
import time

time.sleep(4)

pyautogui.press("down")
pyautogui.keyUp("fn")

pyautogui.hotkey("alt", "right")
pyautogui.hotkey("alt", "right")
pyautogui.hotkey("alt", "right")
pyautogui.hotkey("alt", "right")
pyautogui.hotkey("alt", "right")
pyautogui.keyUp("fn")
pyautogui.press("left")
pyautogui.keyUp("fn")

code = ", func_cpu=64, func_ram=256"
pyautogui.write(code, interval=0.05)
pyautogui.keyUp("fn")

# PUT YOUR CURSOR ON LINE 3 HERE
#   line 1 should be the time import: `from time import time, sleep`
#   line 2 should be blank

# pyautogui.keyUp("fn")
# code = "\ndef simple_test_function(my_input):\n"
# pyautogui.write(code, interval=0.05)

# code = "sleep(1)\n"
# pyautogui.write(code, interval=0.05)

# code = "return my_input\n\n"
# pyautogui.write(code, interval=0.05)
# pyautogui.press("enter")
# pyautogui.keyUp("fn")


# code = "\nmy_inputs = list(range(1_000_000))\n\n"
# pyautogui.write(code, interval=0.05)

# time.sleep(3)

# for _ in range(9):
#     time.sleep(0.01)
#     pyautogui.press("up")

# pyautogui.press("enter")
# pyautogui.keyUp("fn")
# code = "from burla import remote_parallel_map"
# pyautogui.write(code, interval=0.05)
# pyautogui.press("enter")
# pyautogui.keyUp("fn")

# for _ in range(11):
#     time.sleep(0.01)
#     pyautogui.press("down")
# pyautogui.keyUp("fn")

# pyautogui.press("enter")
# pyautogui.keyUp("fn")
# code = "start = time()"
# pyautogui.write(code, interval=0.05)
# pyautogui.press("enter")
# pyautogui.press("enter")
# pyautogui.keyUp("fn")


# code = "list_of_return_values = remote_parallel_map(simple_test_function, my_inputs)"
# pyautogui.write(code, interval=0.05)
# pyautogui.press("enter")
# pyautogui.press("enter")
# pyautogui.keyUp("fn")


# code = 'print(f"Time taken: {time() - start} seconds")'
# pyautogui.write(code, interval=0.05)
# pyautogui.press("enter")
# pyautogui.press("enter")
# pyautogui.keyUp("fn")

# time.sleep(3)


# for _ in range(12):
#     time.sleep(0.01)
#     pyautogui.press("up")
# pyautogui.keyUp("fn")

# for _ in range(20):
#     pyautogui.hotkey("fn", "delete")
# pyautogui.keyUp("fn")
# pyautogui.hotkey("delete")
# pyautogui.keyUp("fn")


# for _ in range(6):
#     time.sleep(0.01)
#     pyautogui.press("down")
# pyautogui.keyUp("fn")

# for _ in range(24):
#     pyautogui.hotkey("fn", "delete")
# pyautogui.keyUp("fn")

# for _ in range(51):
#     pyautogui.press("right")
# pyautogui.keyUp("fn")


# code = ", background=True"
# pyautogui.write(code, interval=0.05)
# pyautogui.keyUp("fn")

# for _ in range(4):
#     time.sleep(0.01)
#     pyautogui.press("down")
# pyautogui.keyUp("fn")
