local web = require('web')


local function gen(request)
    local response = {

    }

    response["header"] = { }
    response["body"] = ""
    response["id"] = request["id"]
    response["type"] = request["type"]
    return response
end

local function http_magick(self,request)
    print(10)
    return self:http("hello")(self,request)
end

local function print_body(self,request)
    print(11)
    local response = gen(request)
    response["body"]="{\"id\":12}"
    self:write_and_close(response)
end

local app = web.application()

app:handler_http("http","hello",print_body)
app:dispatcher("http",http_magick)
app:start()
