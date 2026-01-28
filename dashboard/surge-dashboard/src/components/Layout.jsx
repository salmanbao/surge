import React from "react";
import { Link, useLocation } from "react-router-dom";
import {
  Box,
  Drawer,
  AppBar,
  Toolbar,
  List,
  Typography,
  Divider,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Chip,
  Avatar,
  FormControl,
  Select,
  MenuItem,
  InputLabel,
} from "@mui/material";
import {
  Queue as QueueIcon,
  Code as CodeIcon,
  People as PeopleIcon,
} from "@mui/icons-material";
import surgeLogo from "/surge_logo.svg";

const drawerWidth = 260;

export function Layout({
  children,
  namespaces = [],
  selectedNs,
  onNamespaceChange,
}) {
  return (
    <Box sx={{ display: "flex", height: "100vh" }}>
      <AppBar
        position="fixed"
        sx={{
          width: `calc(100% - ${drawerWidth}px)`,
          ml: `${drawerWidth}px`,
          backgroundColor: "#ffffff",
          color: "#1976d2",
          boxShadow: "0 1px 3px rgba(0,0,0,0.12)",
        }}
      >
        <Toolbar>
          {namespaces.length > 0 && onNamespaceChange && (
            <FormControl size="small" sx={{ minWidth: 200 }}>
              <InputLabel>Namespace</InputLabel>
              <Select
                value={
                  namespaces.includes(selectedNs)
                    ? selectedNs
                    : namespaces[0] || ""
                }
                label="Namespace"
                onChange={(e) => {
                  const newNs = e.target.value;
                  onNamespaceChange(newNs);
                }}
              >
                {namespaces.map((ns) => (
                  <MenuItem key={ns} value={ns}>
                    {ns}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          )}
          <Box sx={{ flexGrow: 1 }} />
        </Toolbar>
      </AppBar>

      <Drawer
        sx={{
          width: drawerWidth,
          flexShrink: 0,
          "& .MuiDrawer-paper": {
            width: drawerWidth,
            boxSizing: "border-box",
            backgroundColor: "#1e293b",
            color: "#ffffff",
          },
        }}
        variant="permanent"
        anchor="left"
      >
        <Toolbar
          sx={{
            display: "flex",
            alignItems: "center",
            justifyContent: "flex-start",
            px: 2,
            py: 1.5,
            borderBottom: "1px solid rgba(255, 255, 255, 0.12)",
          }}
        >
          <img
            src={surgeLogo}
            alt="Surge Logo"
            style={{
              width: 56,
              height: 56,
              objectFit: "contain",
              marginRight: 12,
            }}
          />
          <Box>
            <Typography variant="h6" sx={{ fontWeight: 600, fontSize: "1rem" }}>
              Surge
            </Typography>
            <Typography
              variant="caption"
              sx={{ color: "rgba(255, 255, 255, 0.6)", fontSize: "0.7rem" }}
            >
              Dashboard v1.0
            </Typography>
          </Box>
        </Toolbar>

        <List sx={{ flex: 1, py: 1 }}>
          <NavItem to="/" icon={<QueueIcon />}>
            Queues
          </NavItem>
          <NavItem to="/handlers" icon={<CodeIcon />}>
            Handlers
          </NavItem>
          <NavItem to="/consumers" icon={<PeopleIcon />}>
            Consumers
          </NavItem>
        </List>

        <Box sx={{ p: 2, borderTop: "1px solid rgba(255, 255, 255, 0.12)" }}>
          <Chip
            icon={
              <Box
                sx={{
                  width: 8,
                  height: 8,
                  borderRadius: "50%",
                  bgcolor: "#4caf50",
                }}
              />
            }
            label="System Online"
            size="small"
            sx={{
              backgroundColor: "rgba(255, 255, 255, 0.08)",
              color: "rgba(255, 255, 255, 0.9)",
              fontSize: "0.75rem",
              height: 24,
              "& .MuiChip-icon": {
                marginLeft: 1,
              },
            }}
          />
        </Box>
      </Drawer>

      <Box
        component="main"
        sx={{
          flexGrow: 1,
          bgcolor: "#f5f5f5",
          p: 3,
          width: `calc(100% - ${drawerWidth}px)`,
          overflow: "auto",
          mt: "64px",
        }}
      >
        {children}
      </Box>
    </Box>
  );
}

function NavItem({ to, children, icon }) {
  const location = useLocation();
  const isActive =
    location.pathname === to || (to === "/" && location.pathname === "/");

  return (
    <ListItem disablePadding sx={{ mb: 0.5, px: 1.5 }}>
      <ListItemButton
        component={Link}
        to={to}
        sx={{
          borderRadius: 1,
          minHeight: 44,
          backgroundColor: isActive
            ? "rgba(25, 118, 210, 0.16)"
            : "transparent",
          color: isActive ? "#90caf9" : "rgba(255, 255, 255, 0.7)",
          "&:hover": {
            backgroundColor: isActive
              ? "rgba(25, 118, 210, 0.24)"
              : "rgba(255, 255, 255, 0.08)",
            color: "#ffffff",
          },
          "&.Mui-selected": {
            backgroundColor: "rgba(25, 118, 210, 0.16)",
            color: "#90caf9",
            "&:hover": {
              backgroundColor: "rgba(25, 118, 210, 0.24)",
            },
          },
        }}
        selected={isActive}
      >
        <ListItemIcon
          sx={{
            minWidth: 40,
            color: "inherit",
          }}
        >
          {icon}
        </ListItemIcon>
        <ListItemText
          primary={children}
          primaryTypographyProps={{
            fontSize: "0.875rem",
            fontWeight: isActive ? 600 : 400,
          }}
        />
      </ListItemButton>
    </ListItem>
  );
}
