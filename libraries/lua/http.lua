local function errorf(fmt, ...)
    error(string.format(fmt, ...))
end

local function extend(tbl, tblu, raise)
    local res = {}
    for k, v in pairs(tbl) do
        res[k] = v
    end
    for k, v in pairs(tblu) do
        if raise then
            if res[k] == nil then
                errorf("Unknown option '%s'", k)
            end
        end
        res[k] = v
    end
    return res
end

local function add_handler(self, type, name_handler, handler)
    if type == "http" then
        self.handlers_http[name_handler] = handler
    elseif type == "ws" then
        self.handlers_ws[name_handler] = handler
    else
        --- -TODO
    end
    return self
end

local function add_handler_ws(self, type, name_handler, handler)
    add_handler(self,"ws",name_handler,handler)
    return self
end


local function add_handler_http(self, type, name_handler, handler)
    --print("edc")
    add_handler(self,"http",name_handler,handler)
    return self
end

local function find_handler(self, type, name_handler)
    if type == "http" then
        --print("1234567890")
        return self.handlers_http[name_handler]
    elseif type == "ws" then
        return self.handlers_ws[name_handler]
    else
        --- -TODO
    end
end

local function find_ws_handler(self, name_handler)
    return find_handler(self,"ws",name_handler)
end

local function find_http_handler(self, name_handler)
    --print(4567)
    return find_handler(self,"http",name_handler)
end



local function dispatcher(self, id)
    local type = job_type(id)
    --print("qwertyui")
    print(id)
    --print("qwertyui")
    self.current_id = id;
    if type == 0x00 then
        return self.dispatchers["http"](self)
    elseif type == 0x01 then
        return self.dispatchers["ws"](self)
    else
        --- -TODO
        return
    end

    return

end

local function start(self)
    --print("0qaz")
    if type(self) ~= 'table' then
        error("httpd: usage: httpd:start()")
    end
    --print("1qaz")
    while true do
        local jobs = {}
        --print("2qaz")
        local size = jobs_wait(jobs)
        --print("3qaz")

        if size ~= 0 then
            for key in pairs(jobs) do
                --print("4qaz")
                print(jobs[key])
                dispatcher(self, jobs[key])
                self.current_id = nil
                --print("5qaz")
            end
            --print("6qaz")
            for key in pairs(jobs) do
                jobs[key] = nil
            end
        end
        --print("7qaz")
    end
end

local function add_dispatcher(self, type, handler)
    self.dispatchers[type] = handler
    return self
end

local function body_write(self,body)
    http_body_write(self.current_id,body)
end

local function body_read(self)
    --print("6yhn")
    return http_body_read(self.current_id)
end

local function http_header(self,name)
    --print("9ujm")
    return http_header_read(self.current_id,name)
end

local function finish(self)
    --print("f8888888888888")
    job_close(self.current_id)
    --print("f9999999999999")
    self.current_id = nil
end


local exports = {
    application = function()

        local default = {}

        local self = {
            job_id = {},

            current_id = nil,

            --- methods
            handler_ws = add_handler_ws,
            handler_http = add_handler_http,
            ws = find_ws_handler,
            http = find_http_handler,
            dispatcher = add_dispatcher,
            start = start,
            body_read = body_read,
            finish = finish,
            http_header = http_header,
            --- handler
            dispatchers = {},
            handlers_http = {},
            handlers_ws = {}
        }

        return self
    end
}

return exports