x columns which are hours 0 - 24 (25  0 and 24 are equal)
y latidute -87 to 87 by 3 degrees
           -84
           -81
           -78
           (59 lines)
z = 

/etc/nginx/nginx.conf
proxy_hide_header 'access-control-allow-origin';
  add_header Access-Control-Allow-Origin "*";
proxy_cache_path /var/cache/nginx/proxycache levels=1:2 keys_zone=cache:1m max_size=250m;^M 


        # io_ = io.BytesIO()
        os.chdir(f'/home/ubuntu/experiments/dtm/runs/{execution_id}')
        response = FileResponse(f'DTM20F107Kp_{data}.datx', media_type="text/csv")
        response.headers["Content-Disposition"] = f"attachment; filename=DTM20F107Kp_{execution_id}_{data}.datx"
        return response
        # files = os.listdir()
        # files_to_zip =['input']
        # for file in files:
        #     if file.endswith('.datx'):
        #         files_to_zip.append(file)
        # with zipfile.ZipFile(io_, mode='w') as zip:
        #     for file in files_to_zip:
        #         zip.write(file)
        #     zip.close()
        # return StreamingResponse(
        #     iter([io_.getvalue()]),
        #     media_type="application/zip",
        #     headers={"Content-Disposition": f"attachment;filename=results.zip"}
        #     )

