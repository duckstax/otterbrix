local web = require('http')

local function http_magick(self)
    print(555555555555555)
    return self:http("hello")(self)
end
print(4)
local function print_body(self)
    print(66666666666666)
    print(self:body_read())
    print("Content-Type")
    print(self:http_header("Content-Type"))
    print(77777777777777)
    self:finish()
end

local app = web.application()
app:handler_http("http","hello",print_body)
app:dispatcher("http",http_magick)
app:start()
