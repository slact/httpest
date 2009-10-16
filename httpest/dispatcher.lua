local table, print, assert, setmetatable, rawset, rawget, type, pairs, ipairs, tostring = table, print, assert, setmetatable, rawset, rawget, type, pairs, ipairs, tostring
local socket = require "socket"
local error=error
module(...)

recvt, sendt = {}, {} --select()ed tables of sockets. these must be lists (indexed by consecutive numbers, starting with 1: ipairs() iterable)

do  --helper stuff for sendt and recvt
	local _ins = function(self, sock) --insert socket
		assert(sock, "socket (" .. tostring(sock) .. ") is mandatory")
		if not self.socks[sock] then
			table.insert(self, sock)
			self.socks[sock]=true
		else
			print("tried adding an already present socket")
		end
	end
	local _del = function(self, sock) --delete socket. dangerously O(n)
		assert(sock, "socket is mandatory")
		if self.socks[sock] then
			--- unless socket.select() can do pairs instead of ipairs, i don't see an O(1) solution...
			for i, s in ipairs(self) do
				if sock==s then
					table.remove(self, i)
					self.socks[sock]=nil
					return true
				end
			end
			error("tried to delete socket " .. tostring(sock) .. ", but couldn't find it. this shouldn't happen.")
		else
			--print("tried deleting already deleted socket")
		end
	end
	
	for i, t in pairs{recvt, sendt} do
		setmetatable(t, { __index = { 
			insert = _ins, delete = _del, 
			socks = setmetatable({}, {__mode="k"}), --avoid-duplicates check table. 
		}})
	end
end

local receive_handlers, send_handlers, close_handlers = setmetatable({}, {__mode="k"}), setmetatable({}, {__mode="k"}), setmetatable({}, {__mode="k"})

function register_server(s, accept, close, connection_handlers)
	connection_handlers = type(connection_handlers)=="table" and connection_handlers or {}
	if type(s)=="table" then
		accept=s.accept
		close=s.close
		connection_handlers=s.connection
		s=s.socket
	end
	
	if type(s)~="userdata" then return nil, "invalid socket " .. tostring(s) end
	
	local cl_send, cl_receive, cl_close = connection_handlers.send, connection_handlers.receive, connection_handlers.close
	local register_client = register_client --faster?
	
	if accept then --custom accept callback
		receive_handlers[s]=function(serv) 
			local res, err = accept(serv)
			if res==true then --accepter took care of registering client with the server, apparently
				return true
			else
				return register_client(res, cl_receive, cl_send, cl_close)
			end
		end
	else
		receive_handlers[s]=function(serv) --this will work unless sendt and recvt are weak
			local conn, err = serv:accept()
			if not conn then return nil, err end
			return register_client(res, cl_receive, cl_send, cl_close)
		end
	end
	recvt:insert(s)
	return true
end

function register_client(s, receive, send, close)
	if type(s)=="table" then 
		receive = s.receive
		send = s.send
		close = s.close
		s=s.socket
	end
	if type(s)~="userdata" then return nil, "invalid socket: " .. tostring(socket) end
	s:settimeout(0, 't')
	if receive then
		receive_handlers[s]=receive
		recvt:insert(s)
	end
	send_handlers[s]=send
	close_handlers[s]=close
	return s
end



--little local message buffer
local buffer = setmetatable({}, {__index={
	queue = function(self, id, msg)
		if not self[id] then self[id]={} end
		table.insert(self[id], msg)
		return self[id]
	end,
	dequeue = function(self, id)
		local t = self[id]
		local msg = t and t[1]
		if t then table.remove(t,1) end
		if t and #t==0 then self[id]=nil end
		return msg
	end,
	size = function(self, id)
		return (self[id] and #self[id] or 0)
	end
}})


-- queue a message to be sent to a socket
queue = function(sock, message)
	if type(sock)~="userdata" then return nil, "invalid socket" end
	if buffer:size(sock)==0 then
		sendt:insert(sock, "send")
	end
	buffer:queue(sock, message)
	--print("MESSAGE QUEUE for " .. tostring(sock) .. " is " .. buffer:size(sock))
	return true
end


local dequeue=function(sock) --get a message intended for a socket
	local msg, f = buffer:dequeue(sock)
	if type(msg)=="function" then --yep, messages can be callbacks
		msg, f = msg() --and they can return a message string and a followup callback. grand, eh?
	end
	if buffer:size(sock)==0 then
		sendt:delete(sock)
	end
	return msg, f
end

queue_size = function(sock)
	return buffer:size(sock)
end

queue_empty = function(sock)
	return queue_size(sock)==0
end

function step(select_timeout) --do one select() iteration. return true
	--print("recvt:", #recvt, "sendt:", #sendt)
	local readable, writeable, select_err = socket.select(recvt, sendt, select_timeout/1000)
	for i, sock in ipairs(readable) do
		
		if receive_handlers[sock] then
			local res, err = receive_handlers[sock](sock)
			if err=="closed" then
				close_socket(sock)
			end
		else
			error("receiving for socket with no receive handler")
		end
	end
	
	--handle writeable connections
	for i, sock in ipairs(writeable) do
		local msg, endfunc = dequeue(sock)
		local res, err = (send_handlers[sock] or sock.send)(sock, msg);
		if err=="closed" then
			close_socket(sock)
		end
		if endfunc and type(endfunc)=="function" then endfunc(sock) end
	end
	
	return true, select_err
end

--step() returns false one time.
abort = function()
	local oldstep = step
	step = function()
		step = oldstep
		return false
	end
	return true
end

function loop(select_timeout)
	repeat until not step(select_timeout)
end

--totally close the socket, without flushing its send buffer
close_socket = function(sock) --flush nothing.
	recvt:delete(sock)
	sendt:delete(sock)
	buffer[sock]=nil
	if sock.close then sock:close() end
end
