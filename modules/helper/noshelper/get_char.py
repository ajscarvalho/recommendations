#!/usr/bin/env python
# -*- coding: utf-8 -*-
# found on 

USER_EXIT_CODE = 254

class _Getch:
    """Gets a single character from standard input.  Does not echo to the screen."""
    def __init__(self):
        try:
            self.impl = _GetchWindows()
        except ImportError:
            self.impl = _GetchUnix()

    def __call__(self): return self.impl()


class _GetchUnix:
    def __init__(self):
        import tty, sys

    def __call__(self):
        import sys, tty, termios
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(sys.stdin.fileno())
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        return ch


class _GetchWindows:
    def __init__(self):
        import msvcrt

    def __call__(self):
        import msvcrt
        return msvcrt.getch()


get_char = _Getch()


def wait_break_char(msg=None):
    if msg is None:
        msg = '\n    ---- Execution Paused (press "q" to quit, any other key to continue ----\n'
    
    print (msg)
    ch = get_char()
    #print ("Char", ch)
    if ch == b'q':
        user_exit('QUIT: User pressed q')

    return ch


def user_exit(msg=None):
    if msg: print ("\n{}\n".format(msg))
    import sys; sys.exit(USER_EXIT_CODE)


def is_user_exit(code): return code == USER_EXIT_CODE