// server.js
require('dotenv').config();
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const mongoose = require('mongoose');
const { Redis } = require('@upstash/redis');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const cookieParser = require('cookie-parser');
const app = express();
app.use(express.json());
app.use(cookieParser());
// Serve static frontend files from the "public" directory
app.use(express.static(path.join(__dirname, 'public')));
// JWT Secret Key
const JWT_SECRET = process.env.JWT_SECRET ||
'local_chat_app_development_secret_key_987654321';
// ==========================================
// 1. DATABASE & CACHE INTEGRATION (WITH MOCKS)
// ==========================================
let isMongoConnected = false;
let isRedisConnected = false;
// --- A. MongoDB / Mongoose Setup ---
const mongoUri = process.env.MONGODB_URI;
// In-Memory Database Fallback for development/testing without MongoDB Atlas
const localDbMock = {
 users: [],
 messages: [],
 groups: [],
 async findUser(username) {
 return this.users.find(u => u.username.toLowerCase() ===
username.toLowerCase());
 },
 async getAllUsers() {
 return this.users.map(u => ({ username: u.username, avatar: u.avatar }));
 },
 async createUser(username, hashedPassword, avatar) {
 const newUser = {
 _id: 'mock_u_' + Math.random().toString(36).substr(2, 9),
 username,
 password: hashedPassword,
 avatar: avatar ||
`https://api.dicebear.com/7.x/bottts/svg?seed=${username}`,
 createdAt: new Date()
 };
 this.users.push(newUser);
 return newUser;
 },
 async getRecentMessages(roomId = 'lounge', limit = 50) {
 return this.messages.filter(m => m.roomId === roomId).slice(-limit);
 },
 async saveMessage(sender, content, type = 'chat', roomId = 'lounge') {
 const newMsg = {
 _id: 'mock_m_' + Math.random().toString(36).substr(2, 9),
 sender,
 content,
 type,
 roomId,
 timestamp: new Date()
 };
 this.messages.push(newMsg);
 return newMsg;
 },
 async createGroup(name, creator, members = [], avatar) {
 const newGroup = {
 _id: 'mock_g_' + Math.random().toString(36).substr(2, 9),
 name,
 creator,
 members: Array.from(new Set([creator, ...members])),
 avatar: avatar ||
`https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(name)}&backgroundColor=6a11cb,2575fc&color=ffffff`,
 createdAt: new Date()
 };
 this.groups.push(newGroup);
 return newGroup;
 },
 async getGroupsForUser(username) {
 return this.groups.filter(g => g.members.includes(username));
 },
 async leaveGroup(groupId, username) {
 const group = this.groups.find(g => g._id === groupId);
 if (group) {
 group.members = group.members.filter(m => m !== username);
 }
 return group;
 },
 async updateGroupMembers(groupId, members) {
 const group = this.groups.find(g => g._id === groupId);
 if (group) {
 group.members = Array.from(new Set([group.creator, ...members]));
 }
 return group;
 }
};
// Define Mongoose Schemas if Mongo URI exists
let User;
let Message;
let Group;
if (mongoUri) {
 console.log('[SYSTEM] Attempting to connect to MongoDB Atlas...');
 mongoose.connect(mongoUri)
 .then(() => {
 isMongoConnected = true;
 console.log('[DATABASE] Successfully connected to MongoDB Atlas!');
 })
 .catch(err => {
 console.warn('[DATABASE] MongoDB connection failed! Falling back to INMEMORY DATABASE.', err.message);
 });
 const userSchema = new mongoose.Schema({
 username: { type: String, required: true, unique: true, lowercase: true,
trim: true },
 password: { type: String, required: true },
 avatar: { type: String },
 createdAt: { type: Date, default: Date.now }
 });
 User = mongoose.model('User', userSchema);
 const messageSchema = new mongoose.Schema({
 sender: { type: String, required: true },
 content: { type: String, required: true },
 type: { type: String, default: 'chat' },
 roomId: { type: String, default: 'lounge', index: true },
 timestamp: { type: Date, default: Date.now }
 });
 Message = mongoose.model('Message', messageSchema);
 const groupSchema = new mongoose.Schema({
 name: { type: String, required: true },
 creator: { type: String, required: true },
 members: [{ type: String }],
 avatar: { type: String },
 createdAt: { type: Date, default: Date.now }
 });
 Group = mongoose.model('Group', groupSchema);
} else {
 console.warn('[SYSTEM] MONGODB_URI is not defined. Falling back to IN-MEMORY DATABASE.');
}
// --- B. Upstash Redis Setup ---
const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
let redis;
// In-Memory Redis Fallback for development/testing
const localRedisMock = {
 onlineUsers: new Set(),
 cachedMessages: {},
 async sadd(key, member) {
 this.onlineUsers.add(member);
 return 1;
 },
 async srem(key, member) {
 return this.onlineUsers.delete(member) ? 1 : 0;
 },
 async smembers(key) {
 return Array.from(this.onlineUsers);
 },
 async lrange(key, start, stop) {
 if (!this.cachedMessages[key]) return [];
 return this.cachedMessages[key].slice(start, stop === -1 ? undefined : stop +
1);
 },
 async rpush(key, value) {
 if (!this.cachedMessages[key]) this.cachedMessages[key] = [];
 const parsed = typeof value === 'string' ? JSON.parse(value) : value;
 this.cachedMessages[key].push(parsed);
 return this.cachedMessages[key].length;
 },
 async ltrim(key, start, stop) {
 if (!this.cachedMessages[key]) return 'OK';
 this.cachedMessages[key] = this.cachedMessages[key].slice(start, stop === -1
? undefined : stop + 1);
 return 'OK';
 }
};
if (redisUrl && redisToken) {
	try {
		redis = new Redis({
			url: redisUrl,
			token: redisToken,
		});
		// Validate credentials by pinging the Redis REST API. If the token is invalid
		// Upstash will reject requests — in that case fall back to the in-memory mock.
		(async () => {
			try {
				await redis.ping();
				isRedisConnected = true;
				console.log('[CACHE] Successfully connected to Upstash Redis REST Client!');
			} catch (err) {
				console.warn('[CACHE] Upstash Redis authentication failed! Falling back to IN-MEMORY REDIS MOCK.', err.message);
				redis = null;
				isRedisConnected = false;
			}
		})();
	} catch (err) {
		console.warn('[CACHE] Upstash Redis configuration failed! Falling back to INMEMORY REDIS MOCK.', err.message);
		redis = null;
		isRedisConnected = false;
	}
} else {
	console.warn('[SYSTEM] UPSTASH_REDIS_REST_URL or UPSTASH_REDIS_REST_TOKEN are not defined. Falling back to IN-MEMORY REDIS MOCK.');
}
// Create a safe wrapper around the Upstash client that falls back to the
// in-memory mock if any Upstash command fails (for example WRONGPASS).
const createSafeRedisClient = (remoteClient, fallback) => ({
	async sadd(key, member) {
		try { return await remoteClient.sadd(key, member); }
		catch (err) { console.warn('[CACHE] Upstash failed (sadd), using mock:', err.message); return await fallback.sadd(key, member); }
	},
	async srem(key, member) {
		try { return await remoteClient.srem(key, member); }
		catch (err) { console.warn('[CACHE] Upstash failed (srem), using mock:', err.message); return await fallback.srem(key, member); }
	},
	async smembers(key) {
		try { return await remoteClient.smembers(key); }
		catch (err) { console.warn('[CACHE] Upstash failed (smembers), using mock:', err.message); return await fallback.smembers(key); }
	},
	async lrange(key, start, stop) {
		try { return await remoteClient.lrange(key, start, stop); }
		catch (err) { console.warn('[CACHE] Upstash failed (lrange), using mock:', err.message); return await fallback.lrange(key, start, stop); }
	},
	async rpush(key, value) {
		try { return await remoteClient.rpush(key, value); }
		catch (err) { console.warn('[CACHE] Upstash failed (rpush), using mock:', err.message); return await fallback.rpush(key, value); }
	},
	async ltrim(key, start, stop) {
		try { return await remoteClient.ltrim(key, start, stop); }
		catch (err) { console.warn('[CACHE] Upstash failed (ltrim), using mock:', err.message); return await fallback.ltrim(key, start, stop); }
	}
});

const getRedisClient = () => {
	if (isRedisConnected && redis) return createSafeRedisClient(redis, localRedisMock);
	return localRedisMock;
};
// ==========================================
// 2. AUTHENTICATION MIDDLEWARE & ENDPOINTS
// ==========================================
// Authenticate middleware
const authenticateToken = async (req, res, next) => {
 // Get token from Cookie or Auth Header
 let token = req.cookies.token;
 if (!token && req.headers['authorization']) {
 const authHeader = req.headers['authorization'];
 if (authHeader.startsWith('Bearer ')) {
 token = authHeader.split(' ')[1];
 }
 }
 if (!token) {
 return res.status(401).json({ error: 'Access denied. No token provided.' });
 }
 try {
 const decoded = jwt.verify(token, JWT_SECRET);
 req.user = decoded;
 next();
 } catch (err) {
 res.status(403).json({ error: 'Invalid or expired authentication token.' });
 }
};
// --- REST ROUTES ---
// signup endpoint
app.post('/api/auth/signup', async (req, res) => {
 try {
 let { username, password, avatar } = req.body;
 if (!username || !password) {
 return res.status(400).json({ error: 'Username and password are required.' });
 }
 username = username.trim();
 if (username.length < 3 || username.length > 15) {
 return res.status(400).json({ error: 'Username must be between 3 and 15 characters.' });
 }
 // Check if user already exists
 let userExists = false;
 if (isMongoConnected) {
 const existingUser = await User.findOne({ username:
username.toLowerCase() });
 if (existingUser) userExists = true;
 } else {
 const existingUser = await localDbMock.findUser(username);
 if (existingUser) userExists = true;
 }
 if (userExists) {
 return res.status(400).json({ error: 'Username is already taken.' });
 }
 // Hash password
 const salt = await bcrypt.genSalt(10);
 const hashedPassword = await bcrypt.hash(password, salt);
 // Set default SVG initials avatar if none selected
 if (!avatar) {
 avatar =
`https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(username)}&backg
roundColor=00f2fe,4facfe&color=ffffff`;
 }
 // Save User
 let user;
 if (isMongoConnected) {
 user = new User({
 username: username,
 password: hashedPassword,
 avatar
 });
 await user.save();
 } else {
 user = await localDbMock.createUser(username, hashedPassword, avatar);
 }
 // Create JWT Token
 const token = jwt.sign(
 { id: user._id, username: user.username, avatar: user.avatar },
 JWT_SECRET,
 { expiresIn: '7d' }
 );
 // Store inside cookie
 res.cookie('token', token, {
 httpOnly: true,
 secure: process.env.NODE_ENV === 'production',
 maxAge: 7 * 24 * 60 * 60 * 1000 // 7 days
 });
 // Broadcast new user registration to all active WebSocket clients in realtime
 try {
 const userRegisteredPayload = JSON.stringify({
 type: 'user_registered',
 user: {
 username: user.username,
 avatar: user.avatar
 }
 });
 wss.clients.forEach(c => {
 if (c.readyState === WebSocket.OPEN) {
 c.send(userRegisteredPayload);
 }
 });
 } catch (wsErr) {
 console.warn('[WS BROADCAST ERROR] Failed to broadcast new user signup:',
wsErr.message);
 }
 res.status(201).json({
 success: true,
 token,
 user: {
 id: user._id,
 username: user.username,
 avatar: user.avatar
 }
 });
 } catch (error) {
 console.error('[SIGNUP ERROR]', error);
 res.status(500).json({ error: 'An internal server error occurred during registration.' });
 }
});
// login endpoint
app.post('/api/auth/login', async (req, res) => {
 try {
 let { username, password } = req.body;
 if (!username || !password) {
 return res.status(400).json({ error: 'Username and password are required.' });
 }
 username = username.trim();
 // Find user
 let user;
 if (isMongoConnected) {
 user = await User.findOne({ username: username.toLowerCase() });
 } else {
 user = await localDbMock.findUser(username);
 }
 if (!user) {
 return res.status(400).json({ error: 'Invalid username or password.' });
 }
 // Check password
 const isMatch = await bcrypt.compare(password, user.password);
 if (!isMatch) {
 return res.status(400).json({ error: 'Invalid username or password.' });
 }
 // Create JWT Token
 const token = jwt.sign(
 { id: user._id, username: user.username, avatar: user.avatar },
 JWT_SECRET,
 { expiresIn: '7d' }
 );
 // Set inside cookie
 res.cookie('token', token, {
 httpOnly: true,
 secure: process.env.NODE_ENV === 'production',
 maxAge: 7 * 24 * 60 * 60 * 1000 // 7 days
 });
 res.status(200).json({
 success: true,
 token,
 user: {
 id: user._id,
 username: user.username,
 avatar: user.avatar
 }
 });
 } catch (error) {
 console.error('[LOGIN ERROR]', error);
 res.status(500).json({ error: 'An internal server error occurred during login.' });
 }
});
// get current user details
app.get('/api/auth/me', authenticateToken, async (req, res) => {
 res.status(200).json({
 success: true,
 user: req.user
 });
});
// logout endpoint to clear cookie
app.post('/api/auth/logout', (req, res) => {
 res.clearCookie('token');
 res.status(200).json({ success: true, message: 'Logged out successfully.' });
});
// Get list of all registered users
app.get('/api/users', authenticateToken, async (req, res) => {
 try {
 let usersList = [];
 if (isMongoConnected) {
 const dbUsers = await User.find({}, 'username avatar');
 usersList = dbUsers.map(u => ({ username: u.username, avatar: u.avatar
}));
 } else {
 usersList = await localDbMock.getAllUsers();
 }
 res.status(200).json({ success: true, users: usersList });
 } catch (error) {
 console.error('[GET USERS ERROR]', error);
 res.status(500).json({ error: 'Failed to retrieve users.' });
 }
});
// Get groups for the logged-in user
app.get('/api/groups', authenticateToken, async (req, res) => {
 try {
 let groupsList = [];
 if (isMongoConnected) {
 groupsList = await Group.find({ members: req.user.username });
 } else {
 groupsList = await localDbMock.getGroupsForUser(req.user.username);
 }
 res.status(200).json({ success: true, groups: groupsList });
 } catch (error) {
 console.error('[GET GROUPS ERROR]', error);
 res.status(500).json({ error: 'Failed to retrieve groups.' });
 }
});
// Create a new group
app.post('/api/groups', authenticateToken, async (req, res) => {
 try {
 const { name, members } = req.body;
 if (!name || !name.trim()) {
 return res.status(400).json({ error: 'Group name is required.' });
 }
 if (!members || members.length === 0) {
 return res.status(400).json({ error: 'At least one member must be selected to create a group.' });
 }

 const creator = req.user.username;
 let finalMembers = Array.from(new Set([creator, ...(members || [])]));

 const groupAvatar = `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(name.trim())}&backgroundColor=6a11cb,2575fc&color=ffffff`;
 let newGroup;
 if (isMongoConnected) {
 newGroup = new Group({
 name: name.trim(),
 creator,
 members: finalMembers,
 avatar: groupAvatar
 });
 await newGroup.save();
 } else {
 newGroup = await localDbMock.createGroup(name.trim(), creator,
finalMembers, groupAvatar);
 }

 // Broadcast WebSocket notification to all online members of this group
 const groupCreatedPayload = JSON.stringify({
 type: 'group_created',
 group: newGroup
 });

 finalMembers.forEach(memberUsername => {
 const socketSet = userSockets.get(memberUsername);
 if (socketSet) {
 socketSet.forEach(s => {
 if (s.readyState === WebSocket.OPEN) {
 s.send(groupCreatedPayload);
 }
 });
 }
 });

 res.status(201).json({ success: true, group: newGroup });
 } catch (error) {
 console.error('[CREATE GROUP ERROR]', error);
 res.status(500).json({ error: 'Failed to create group.' });
 }
});
// Leave a group
app.post('/api/groups/:id/leave', authenticateToken, async (req, res) => {
 try {
 const groupId = req.params.id;
 const username = req.user.username;
 let group;
 if (isMongoConnected) {
 group = await Group.findById(groupId);
 if (group) {
 if (group.creator === username) {
 return res.status(400).json({ error: 'Group creator cannot leave. You must delete the group instead.' });
 }
 group.members = group.members.filter(m => m !== username);
 await group.save();
 }
 } else {
 const tempGroup = localDbMock.groups.find(g => g._id === groupId);
 if (tempGroup && tempGroup.creator === username) {
 return res.status(400).json({ error: 'Group creator cannot leave. You must delete the group instead.' });
 }
 group = await localDbMock.leaveGroup(groupId, username);
 }
 if (!group) return res.status(404).json({ error: 'Group not found.' });
 // Broadcast system message to group
 const broadcastPayload = JSON.stringify({
 type: 'chat',
 roomId: `group_${group._id}`,
 username: 'System',
 avatar: 'https://api.dicebear.com/7.x/bottts/svg?seed=system',
 message: `@${username} left the group.`,
 timestamp: new Date()
 });

 group.members.forEach(u => {
 const socketSet = userSockets.get(u);
 if (socketSet) {
 socketSet.forEach(s => {
 if (s.readyState === WebSocket.OPEN) {
 s.send(broadcastPayload);
 }
 });
 }
 });
 res.status(200).json({ success: true, group });
 } catch (err) {
 console.error('[LEAVE GROUP ERROR]', err);
 res.status(500).json({ error: 'Failed to leave group.' });
 }
});
// Update group members
app.put('/api/groups/:id/members', authenticateToken, async (req, res) => {
 try {
 const groupId = req.params.id;
 const { members } = req.body;
 const username = req.user.username;
 let group;
 if (!members || members.length === 0) {
 return res.status(400).json({ error: 'Group must have at least one member.' });
 }
 // We only allow creator to update members for simplicity
 if (isMongoConnected) {
 group = await Group.findById(groupId);
 if (group) {
 if (group.creator !== username) return res.status(403).json({ error: 'Only the creator can manage members.' });

 // Add creator automatically to prevent accidental self-removal
 group.members = Array.from(new Set([group.creator, ...members]));
 await group.save();
 }
 } else {
 const tempGroup = localDbMock.groups.find(g => g._id === groupId);
 if (tempGroup && tempGroup.creator !== username) return res.status(403).json({ error: 'Only the creator can manage members.' });
 group = await localDbMock.updateGroupMembers(groupId, members);
 }
 if (!group) return res.status(404).json({ error: 'Group not found.' });
 // Broadcast system message to group
 const broadcastPayload = JSON.stringify({
 type: 'chat',
 roomId: `group_${group._id}`,
 username: 'System',
 avatar: 'https://api.dicebear.com/7.x/bottts/svg?seed=system',
 message: `@${username} updated the group members.`,
 timestamp: new Date()
 });

 group.members.forEach(u => {
 const socketSet = userSockets.get(u);
 if (socketSet) {
 socketSet.forEach(s => {
 if (s.readyState === WebSocket.OPEN) {
 s.send(broadcastPayload);
 }
 });
 }
 });
 // Also broadcast group_updated so clients refresh their member lists
 const updatePayload = JSON.stringify({ type: 'group_updated', group });
 group.members.forEach(u => {
 const socketSet = userSockets.get(u);
 if (socketSet) {
 socketSet.forEach(s => {
 if (s.readyState === WebSocket.OPEN) {
 s.send(updatePayload);
 }
 });
 }
 });
 res.status(200).json({ success: true, group });
 } catch (err) {
 console.error('[UPDATE MEMBERS ERROR]', err);
 res.status(500).json({ error: 'Failed to update members.' });
 }
});
// ==========================================
// 3. WEBSOCKET SERVER & REALTIME LOGIC
// ==========================================
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
// Map of active connections: username -> Set of sockets (handles multiple tabs for
// the same user)
const userSockets = new Map();
// Helper to broadcast presence updates
async function broadcastOnlineUsers() {
 try {
 const client = getRedisClient();
 const onlineUsers = await client.smembers('online_users');

// Construct detail list of online users by getting their avatars from
// currently connected sockets (or fallback)
 const onlineUserList = onlineUsers.map(username => {
 const socketSet = userSockets.get(username);
 let avatar = '';
 if (socketSet && socketSet.size > 0) {
 // Get avatar from the first active socket
 const firstSocket = Array.from(socketSet)[0];
 avatar = firstSocket.avatar;
 }
 if (!avatar) {
 avatar =
`https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(username)}&backg
roundColor=00f2fe,4facfe&color=ffffff`;
 }
 return { username, avatar };
 });
 const payload = JSON.stringify({
 type: 'online_list',
 users: onlineUserList
 });
 wss.clients.forEach(client => {
 if (client.readyState === WebSocket.OPEN) {
 client.send(payload);
 }
 });
 } catch (err) {
 console.error('[PRESENCE BROADCAST ERROR]', err);
 }
}
wss.on('connection', async (ws, req) => {
 // Authenticate WS connection using URL token
 // e.g. ws://localhost:3000?token=xxxx
 const urlParams = new URLSearchParams(req.url.split('?')[1]);
 const token = urlParams.get('token');
 let decodedUser;
 try {
 if (!token) throw new Error('No authentication token provided.');
 decodedUser = jwt.verify(token, JWT_SECRET);
 } catch (err) {
 console.log('[WS] Connection rejected: Unauthenticated client.',
err.message);
 ws.send(JSON.stringify({
 type: 'system',
 message: 'Authentication failed. Closing connection.'
 }));
 ws.close(4001, 'Unauthorized');
 return;
 }
 const { username, avatar } = decodedUser;
 ws.username = username;
 ws.avatar = avatar;
 // Track active connection
 if (!userSockets.has(username)) {
 userSockets.set(username, new Set());
 }
 userSockets.get(username).add(ws);
 console.log(`[WS] User ${username} connected. Total client tabs:
${userSockets.get(username).size}`);
 // Add to Redis online list
 const client = getRedisClient();
 await client.sadd('online_users', username);
 // Welcome system message
 ws.send(JSON.stringify({
 type: 'system',
 message: `Welcome to the secure chat, @${username}! Connection
authenticated.`
 }));
 // Broadcast updated online presence list
 await broadcastOnlineUsers();
 // Send recent message history to the newly connected user
 try {
 const cacheKey = 'chat_history:lounge';
 const cachedRaw = await client.lrange(cacheKey, 0, -1);
 let history = [];
 if (cachedRaw && cachedRaw.length > 0) {
 history = cachedRaw.map(item => typeof item === 'string' ?
JSON.parse(item) : item);
 } else {
 // Redis cache miss: Query MongoDB (or localDbMock) and fill cache
 if (isMongoConnected) {
 const dbMsgs = await Message.find({ roomId: 'lounge' }).sort({
timestamp: -1 }).limit(50);
 history = dbMsgs.reverse().map(m => ({
 type: 'chat',
 roomId: 'lounge',
 username: m.sender,
 message: m.content,
 timestamp: m.timestamp,
 avatar:
`https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(m.sender)}&backg
roundColor=00f2fe,4facfe&color=ffffff` // Fallback/generate
 }));
 } else {
 history = await localDbMock.getRecentMessages('lounge', 50);
 }
 // Fill cache back asynchronously
 for (const msg of history) {
 await client.rpush(cacheKey, msg);
 }
 await client.ltrim(cacheKey, -50, -1); // Keep max 50 in cache
 }
 // Send history to user
 if (history.length > 0) {
 ws.send(JSON.stringify({
 type: 'history',
 roomId: 'lounge',
 messages: history
 }));
 }
 } catch (err) {
 console.error('[WS] Failed to load chat history:', err.message);
 }
 // Handle messages
 ws.on('message', async (rawData) => {
 try {
 const parsedData = JSON.parse(rawData.toString());

 // Utility to check group membership
 const checkGroupMembership = async (roomId, uname) => {
 if (!roomId.startsWith('group_')) return true;
 const groupId = roomId.replace('group_', '');
 if (isMongoConnected) {
 if (mongoose.Types.ObjectId.isValid(groupId)) {
 const grp = await Group.findById(groupId);
 return grp && grp.members.includes(uname);
 }
 return false;
 } else {
 const grp = localDbMock.groups.find(g => g._id === groupId);
 return grp && grp.members.includes(uname);
 }
 };

 // A. Handle History Fetch Request
 if (parsedData.type === 'get_history') {
 const targetRoomId = parsedData.roomId || 'lounge';
 if (!(await checkGroupMembership(targetRoomId, username))) {
 ws.send(JSON.stringify({ type: 'system', message: 'Access denied: You are not a member of this group.' }));
 return;
 }
 const cacheKey = `chat_history:${targetRoomId}`;
 const cachedRaw = await client.lrange(cacheKey, 0, -1);
 let history = [];
 if (cachedRaw && cachedRaw.length > 0) {
 history = cachedRaw.map(item => typeof item === 'string' ?
JSON.parse(item) : item);
 } else {
 if (isMongoConnected) {
 const dbMsgs = await Message.find({ roomId: targetRoomId
}).sort({ timestamp: -1 }).limit(50);
 history = dbMsgs.reverse().map(m => ({
 type: 'chat',
roomId: targetRoomId,
username: m.sender,
 message: m.content,
 timestamp: m.timestamp,
 avatar:
`https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(m.sender)}&backg
roundColor=00f2fe,4facfe&color=ffffff`
 }));
 } else {
 history = await localDbMock.getRecentMessages(targetRoomId,
50);
 }
 for (const msg of history) {
 await client.rpush(cacheKey, msg);
 }
 await client.ltrim(cacheKey, -50, -1);
 }
 ws.send(JSON.stringify({
 type: 'history',
 roomId: targetRoomId,
 messages: history
 }));
 return;
 }
 // B. Handle New Message Sending
 const content = parsedData.message;
 if (!content || content.trim() === '') return;
 const targetRoomId = parsedData.roomId || 'lounge';
 if (!(await checkGroupMembership(targetRoomId, username))) {
 ws.send(JSON.stringify({ type: 'system', message: 'Access denied: You are not a member of this group.' }));
 return;
 }
 // Form message payload
 const payload = {
 type: 'chat',
 roomId: targetRoomId,
 username: username,
 avatar: avatar,
 message: content.trim(),
 timestamp: new Date()
 };
 // Save to Database (MongoDB or localMock)
 if (isMongoConnected) {
 const newDbMsg = new Message({
 sender: username,
 content: content.trim(),
 roomId: targetRoomId
 });
 await newDbMsg.save();
 } else {
 await localDbMock.saveMessage(username, content.trim(), 'chat',
targetRoomId);
 }
 // Push to Redis Cache (trim to keep last 50)
 const cacheKey = `chat_history:${targetRoomId}`;
 await client.rpush(cacheKey, payload);
 await client.ltrim(cacheKey, -50, -1);
 // Broadcast routing logic
 const broadcastPayload = JSON.stringify(payload);
 if (targetRoomId === 'lounge') {
 wss.clients.forEach(c => {
 if (c.readyState === WebSocket.OPEN) {
 c.send(broadcastPayload);
 }
 });
 } else if (targetRoomId.startsWith('dm_')) {
 let otherUser = targetRoomId.substring(3);
 if (otherUser.startsWith(`${username}_`)) {
 otherUser = otherUser.substring(username.length + 1);
 } else if (otherUser.endsWith(`_${username}`)) {
 otherUser = otherUser.substring(0, otherUser.length -
username.length - 1);
 }

 const parts = [username, otherUser];
 parts.forEach(u => {
 const socketSet = userSockets.get(u);
 if (socketSet) {
 socketSet.forEach(s => {
 if (s.readyState === WebSocket.OPEN) {
 s.send(broadcastPayload);
 }
 });
 }
 });
 } else if (targetRoomId.startsWith('group_')) {
 const groupId = targetRoomId.replace('group_', '');
 let groupMembers = [];
 if (isMongoConnected) {
 if (mongoose.Types.ObjectId.isValid(groupId)) {
 const grp = await Group.findById(groupId);
 if (grp) groupMembers = grp.members;
 }
 } else {
 const grp = localDbMock.groups.find(g => g._id === groupId);
 if (grp) groupMembers = grp.members;
 }
 groupMembers.forEach(u => {
 const socketSet = userSockets.get(u);
 if (socketSet) {
 socketSet.forEach(s => {
 if (s.readyState === WebSocket.OPEN) {
 s.send(broadcastPayload);
 }
 });
 }
 });
 }
 } catch (error) {
 console.error('[WS] Error processing message:', error.message);
 }
 });
 // Close Handler
 ws.on('close', async () => {
 const socketSet = userSockets.get(username);
 if (socketSet) {
 socketSet.delete(ws);
 if (socketSet.size === 0) {
 userSockets.delete(username);
 // Remove from online list in Redis since all user tabs are closed
 await client.srem('online_users', username);
 console.log(`[WS] User ${username} fully logged off.`);
 // Broadcast updated list
 await broadcastOnlineUsers();
 } else {
 console.log(`[WS] Closed a tab for ${username}. Remaining tabs:
${socketSet.size}`);
 }
 }
 });
});
// REST push notification route
app.post('/api/notify', authenticateToken, async (req, res) => {
 const { notification } = req.body;
 if (!notification) {
 return res.status(400).json({ error: "Missing 'notification' parameter in request body." });
 }
 console.log(`[HTTP REST] Admin notification broadcast: ${notification}`);
 const notificationPayload = JSON.stringify({
 type: 'notification',
 message: notification,
 timestamp: new Date()
 });
 let activeReceivers = 0;
 wss.clients.forEach(client => {
 if (client.readyState === WebSocket.OPEN) {
 client.send(notificationPayload);
 activeReceivers++;
 }
 });
 res.status(200).json({
 success: true,
 message: `Notification pushed to ${activeReceivers} active client sessions.`
 });
});
// Start the combined HTTP and WebSocket Server
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
 console.log(`[SYSTEM] Server listening on http://localhost:${PORT}`);
 console.log(`[SYSTEM] Ready for Local Sandbox connection! Mocks enabled for zeroconfig startup.`);
});