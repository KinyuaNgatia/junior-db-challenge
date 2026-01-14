# Railway Deployment Guide

## Step-by-Step Instructions

### 1. Login to Railway

```bash
railway login
```

This will open your browser. Authorize the CLI to access your Railway account.

### 2. Initialize Railway Project

```bash
railway init
```

- Choose: **"Create new project"**
- Project name: `junior-db-challenge` (or any name you prefer)

### 3. Link to Current Directory

Railway will automatically detect your Go application and create the project.

### 4. Deploy

```bash
railway up
```

This will:

- Build your Go application
- Deploy it to Railway
- Give you a deployment URL

### 5. Get Your Backend URL

```bash
railway domain
```

This will show your app's public URL, something like:
`https://junior-db-challenge-production.up.railway.app`

### 6. Copy the URL

Once you have the URL, **copy it** and let me know. I'll update the frontend to use it.

---

## What I've Already Done ✅

- ✅ Added CORS support (allows GitHub Pages to call your API)
- ✅ Made PORT dynamic (Railway requirement)
- ✅ Created `Procfile` (tells Railway how to run your app)
- ✅ Created `railway.toml` (Railway configuration)

---

## Next Steps (After You Deploy)

1. You run the commands above
2. You give me the Railway URL
3. I update `docs/index.html` to use that URL instead of `localhost:8080`
4. We push to GitHub
5. GitHub Pages will work! 🎉

---

## Troubleshooting

**If `railway login` fails:**

- Make sure you're logged into railway.app in your browser
- Try: `railway logout` then `railway login` again

**If deployment fails:**

- Check: `railway logs`
- The app needs Go 1.23+ (Railway should auto-detect)

**If you see "command not found":**

- Restart your terminal after installing Railway CLI
- Or use full path: `npx @railway/cli login`
