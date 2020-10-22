#!/bin/bash
exec gunicorn -w 4 --bind 0.0.0.0:80 app:app