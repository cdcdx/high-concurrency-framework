

ab -n 10000 -c 100 -k -r -s 30 \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3ODMzMTA0NzksImlhdCI6MTc4MzMwMzI3OSwidXNlcl9pZCI6MiwidXNlcm5hbWUiOiJ0ZXN0dXNlciJ9.ydPlrOtdSb4IrY11m6Tj_08lXecgk0r9Hs1Ye_N5xFY" \
  http://localhost:8080/api/v1/users/10001/profile

ab -n 10000 -c 100 -k -r -s 30 \
  -p tests/order.json -T application/json \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3ODMzMTA0NzksImlhdCI6MTc4MzMwMzI3OSwidXNlcl9pZCI6MiwidXNlcm5hbWUiOiJ0ZXN0dXNlciJ9.ydPlrOtdSb4IrY11m6Tj_08lXecgk0r9Hs1Ye_N5xFY" \
  http://localhost:8080/api/v1/orders/sync

ab -n 10000 -c 100 -k -r -s 30 \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3ODMzMTA0NzksImlhdCI6MTc4MzMwMzI3OSwidXNlcl9pZCI6MiwidXNlcm5hbWUiOiJ0ZXN0dXNlciJ9.ydPlrOtdSb4IrY11m6Tj_08lXecgk0r9Hs1Ye_N5xFY" \
  http://localhost:8080/api/v1/users/10001/profile
