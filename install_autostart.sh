#!/bin/bash
# Installs the GA4 Intelligence app as a macOS LaunchAgent so agents
# run continuously while your PC is on (started automatically at login).
set -e

PLIST_NAME="cz.promogen.ga4intel.plist"
SOURCE="$(cd "$(dirname "$0")" && pwd)/launch_agent.plist"
TARGET="$HOME/Library/LaunchAgents/$PLIST_NAME"

mkdir -p "$HOME/Library/LaunchAgents"

if [ -f "$TARGET" ]; then
    echo "Reloading existing LaunchAgent..."
    launchctl unload "$TARGET" 2>/dev/null || true
fi

cp "$SOURCE" "$TARGET"
launchctl load "$TARGET"

echo ""
echo "✓ GA4 Intelligence is now running 24/7 on this PC."
echo "  Started automatically at login. Will auto-restart if it crashes."
echo ""
echo "  URL:        http://localhost:8060"
echo "  Logs:       tail -f $(cd "$(dirname "$0")" && pwd)/data/launchagent.log"
echo "  Stop:       launchctl unload $TARGET"
echo "  Status:     launchctl list | grep ga4intel"
