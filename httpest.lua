local http, socket = (require "httpest.sillyhttp"), (require "socket")
local dispatcher = (require "httpest.dispatcher")
local time, write, pcall, assert = os.time, io.write, pcall, assert
local tostring, tinsert, tremove = tostring, table.insert, table.remove
local setmetatable, tonumber, print = setmetatable, tonumber, print
local next, pairs = next, pairs
module(...)
local tcp=socket.tcp
local function receive(sock)
	local l, err = sock:receive(response:whatnext())
	if not l then 
		return nil, err 
	end
	response:receive(l)
	if response.complete then
		if response.invalid then 
			print(response.invalid) 
		end
		--TODO: check connection-close and such
		dispatcher.close_socket(sock)
		if p.complete then 
			p.complete(response) 
			return l
		else
			print("nothing to complete")
		end
	end
	return l
end 


local clientpool, complete_callbacks = {}, setmetatable({}, {__mode='k'})
local free_client_hosts = setmetatable({}, {
	__index = function(t,k) 
		local v = setmetatable({}, {__mode='k'}) 
		t[k]=v 
		return v 
	end
})
local clientcount = 0

local function killclient(host, sock)
	free_client_hosts[host][sock]=nil
	dispatcher.close_socket(sock)
	clientpool[sock], complete_callbacks[sock] = nil, nil
	clientcount = clientcount - 1
	
end

local newresponse = sillyhttp.response.parser
local function newclient(host, timeout)
	local s, err= tcp()
	if not s then return nil, err end
	local hostname, port = host:match("([^:]+):?(%d*)")
	s:settimeout(1, 't')
	local succ, err = s:connect(hostname, tonumber(port) or 80)
	if not succ then
		print(err)
		return nil, err 
	end
	local response = newresponse()
	local receive = function(sock) 
		local l, err = sock:receive(response:whatnext())
		if not l then 
			
			return nil, err 
		end
		--print(">> " .. l)
		response:receive(l)
		if response.complete then
			local r = response
			response = newresponse()
			if r.invalid then 
				print("INVALID RESPONSE", r.invalid) 
				killclient(host, sock)
				return complete_callbacks[sock](r, "invalid")
			end
			return complete_callbacks[sock](r)
		end
		return l
	end
	dispatcher.register_client{
		socket=s,
		receive=receive
	}
	clientpool[s]=host
	clientcount = clientcount + 1
	return s
end

local function getclient(host)
	local sock = (next(free_client_hosts[host]))
	if not sock then 
		return newclient(host)
	end
	free_client_hosts[host][sock]=nil
	return sock
end

function freeclient(sock)
	complete_callbacks[sock]=nil
	free_client_hosts[clientpool[sock]][sock]=true
end

function request(p)
	local req = sillyhttp.request.new()
	req.headers=p.headers or {}
	req:seturl(p.uri or p.url)
	req:setmethod(p.method)
	req:setdata(p.data)
	--what with the url checking
	local host, port = (req:getheader("host") or ""):match("([^:]+):?(%d*)")
	local auth = host .. ":" .. (#port==0 and 80 or port)
	local sock, err = getclient(auth)
	if not sock then return nil, err end
	local callback=p.complete
	dispatcher.queue(sock, req:finish())
	complete_callbacks[sock]=function(r, status)
		freeclient(sock)
		if (r:getheader("connection") or ""):lower():match('close') then
			killclient(auth, sock)
		end
		return callback and callback(r, status, sock)
	end
	return sock
end

function abort_request(sock)
	killclient(clientpool[sock], sock)
end

local tests, timers = {}, {}
function timer(msec, callback)
	local now = time()
	local cb = function()
		return callback()
	end
	timers[callback]=now+msec/1000
end
function killtimer(callback)
	timers[callback]=nil
end

local function runtest(name, callback)
	print("test: " .. name)
	local succ, res, err = pcall(callback)
	if res==true then 
		print(" ok")
	elseif res==false then
		print(" fail")
	elseif not succ then
		print(" fail: " .. (res or "(?)"))
	end
end 

function run()
	while #tests>0 or #timers>0 or (next(complete_callbacks)) do
		--print(#tests, #timers, (next(complete_callbacks)))
		local now = time()
		local f, when = next(timers)
		while f~=nil do
			local lf, lwhen = f, when
			f, when = next(timers, f)
			if lwhen < now then
				timers[lf]=nil
				lf()
			end
		end
		
		if #tests > 0 and not (next(complete_callbacks)) then
			local nextname, nexttest = next(tests[1])
			if not nextname then
				return false
			end
			runtest(nextname, nexttest)
			tremove(tests, 1)
		end
		dispatcher.step(500)
	end
end

function addtest(name, test)
	tinsert(tests, {[name]=test})
	return test
end