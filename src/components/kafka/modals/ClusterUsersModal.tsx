import { useEffect, useState } from 'react';
import { Check, KeyRound, Pencil, Plus, Trash2, X } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '../../ui/button';
import { Dialog, DialogHeader, DialogTitle, DialogDescription } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import { ClusterUser, KafkaCluster, newId } from '../types';
import * as api from '../api';

interface ClusterUsersModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  cluster: KafkaCluster | null;
  /** Учётка, под которой держится текущее подключение, если это тот кластер. */
  activeUserId?: string | null;
  /** Кластер уже записан на диск бэкендом — колбэк только обновляет состояние. */
  onChanged: (cluster: KafkaCluster) => void;
  /** Переподключиться под этой учёткой: её креды только что изменились. */
  onReconnect?: (user: ClusterUser) => void;
  /** Разорвать подключение: учётки, на которой оно держалось, больше нет. */
  onDisconnect?: () => void;
  onBack?: () => void;
}

export function ClusterUsersModal({
  open,
  onOpenChange,
  cluster,
  activeUserId,
  onChanged,
  onReconnect,
  onDisconnect,
  onBack,
}: ClusterUsersModalProps) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [busy, setBusy] = useState(false);

  // Правка и удаление — по одной строке за раз: два открытых подтверждения
  // в списке из десятка учёток означают удаление не той.
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editUsername, setEditUsername] = useState('');
  const [editPassword, setEditPassword] = useState('');
  const [confirmingId, setConfirmingId] = useState<string | null>(null);

  // Модалку открывают и на другом кластере — недописанная форма от прошлого
  // раза здесь была бы чужой.
  useEffect(() => {
    setUsername('');
    setPassword('');
    setEditingId(null);
    setConfirmingId(null);
  }, [cluster?.id, open]);

  if (!cluster) return null;

  const users = cluster.users ?? [];

  const handleAdd = async () => {
    const name = username.trim();
    if (!name) return;
    if (users.some((u) => u.username === name)) {
      toast.error(`User "${name}" already exists on this cluster`);
      return;
    }
    try {
      setBusy(true);
      const updated = await api.saveClusterUser(
        cluster.id,
        { id: newId(), username: name, has_password: false },
        password,
      );
      onChanged(updated);
      setUsername('');
      setPassword('');
      toast.success(`User "${name}" added`);
    } catch (e) {
      console.error('Failed to add user', e);
      toast.error(`Failed to add user: ${e}`);
    } finally {
      setBusy(false);
    }
  };

  const startEditing = (user: ClusterUser) => {
    setConfirmingId(null);
    setEditingId(user.id);
    setEditUsername(user.username);
    setEditPassword('');
  };

  const handleSaveEdit = async (user: ClusterUser) => {
    const name = editUsername.trim();
    if (!name) return;
    try {
      setBusy(true);
      const updated = await api.saveClusterUser(
        cluster.id,
        { ...user, username: name },
        // Пустое поле означает «оставить сохранённый пароль», а не «стереть»:
        // форма открывается пустой на каждой правке имени.
        editPassword ? editPassword : undefined,
      );
      onChanged(updated);
      setEditingId(null);

      // Правили ту учётку, под которой держится подключение, — значит правили
      // действующие креды. Оставить после этого старое соединение значит
      // сказать «сохранено» и ничего не изменить: смена пароля молча не
      // применялась, пока не переключишься на соседнюю учётку и обратно.
      const saved = updated.users.find((u) => u.id === user.id);
      if (saved && user.id === activeUserId) {
        onReconnect?.(saved);
      }

      toast.success(`User "${name}" updated`);
    } catch (e) {
      console.error('Failed to update user', e);
      toast.error(`Failed to update user: ${e}`);
    } finally {
      setBusy(false);
    }
  };

  const handleDelete = async (user: ClusterUser) => {
    try {
      setBusy(true);
      const wasActive = user.id === activeUserId;
      const updated = await api.deleteClusterUser(cluster.id, user.id);
      onChanged(updated);
      setConfirmingId(null);

      // Удалили учётку, на которой держалось подключение. Пароля больше нет, а
      // живой сеанс аутентифицирован им один раз при подключении и продолжал бы
      // читать как ни в чём не бывало — то есть по кредам, которых уже не
      // существует. Переподключаться тут не под кем: молча пересесть на
      // соседнюю учётку значило бы сменить пользователю права, не спросив.
      if (wasActive) onDisconnect?.();

      toast.success(`User "${user.username}" deleted`);
    } catch (e) {
      console.error('Failed to delete user', e);
      toast.error(`Failed to delete user: ${e}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContentNoClose className="max-w-2xl max-h-[85vh] bg-surface border-edge text-slate-50 flex flex-col">
        <DialogHeader className="border-b border-edge pb-4 flex-shrink-0">
          <DialogTitle className="font-mono text-soft text-lg">
            Users · {cluster.name}
          </DialogTitle>
          <DialogDescription className="text-sm text-dim">
            Kafka logins for this cluster. Passwords are kept in the system keychain.
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 overflow-auto space-y-6 pt-4 pr-1">
          {/* Добавление */}
          <div className="space-y-3 border border-edge rounded-lg p-4">
            <h3 className="font-mono text-brand text-sm">Add user</h3>
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label className="font-mono text-sm text-soft">Username</Label>
                <Input
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && handleAdd()}
                  placeholder="Enter username"
                  className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
                />
              </div>
              <div className="space-y-2">
                <Label className="font-mono text-sm text-soft">Password</Label>
                <Input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && handleAdd()}
                  placeholder="Enter password"
                  className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
                />
              </div>
            </div>
            <Button
              onClick={handleAdd}
              disabled={busy || !username.trim()}
              className="bg-brand text-surface hover:bg-brand-hover font-mono disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <Plus className="size-4 mr-2" />
              Add
            </Button>
          </div>

          {/* Список */}
          <div className="space-y-2">
            {users.length === 0 && (
              <div className="text-center py-8 font-mono text-dim text-sm">
                No users yet — add one above to connect to this cluster
              </div>
            )}

            {users.map((user) => {
              const isEditing = editingId === user.id;
              const isConfirming = confirmingId === user.id;

              if (isEditing) {
                return (
                  <div key={user.id} className="border border-edge rounded-lg p-4 space-y-3">
                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <Label className="font-mono text-sm text-soft">Username</Label>
                        <Input
                          value={editUsername}
                          onChange={(e) => setEditUsername(e.target.value)}
                          className="bg-surface border-edge text-slate-50 font-mono"
                        />
                      </div>
                      <div className="space-y-2">
                        <Label className="font-mono text-sm text-soft">Password</Label>
                        <Input
                          type="password"
                          value={editPassword}
                          onChange={(e) => setEditPassword(e.target.value)}
                          placeholder={
                            user.has_password
                              ? 'Saved in keychain — leave blank to keep'
                              : 'Enter password'
                          }
                          className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
                        />
                      </div>
                    </div>
                    <div className="flex gap-2">
                      <Button
                        onClick={() => handleSaveEdit(user)}
                        disabled={busy || !editUsername.trim()}
                        size="sm"
                        className="bg-brand text-surface hover:bg-brand-hover font-mono disabled:opacity-50"
                      >
                        <Check className="size-4 mr-1" />
                        Save
                      </Button>
                      <Button
                        onClick={() => setEditingId(null)}
                        variant="outline"
                        size="sm"
                        className="bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono"
                      >
                        <X className="size-4 mr-1" />
                        Cancel
                      </Button>
                    </div>
                  </div>
                );
              }

              return (
                <div
                  key={user.id}
                  className="flex items-center justify-between gap-3 border border-edge rounded-lg px-4 py-3"
                >
                  <div className="min-w-0">
                    <div className="flex items-center gap-2">
                      {user.id === activeUserId && (
                        <span
                          className="size-2 rounded-full bg-green-500 shrink-0"
                          title="Connected as this user"
                        />
                      )}
                      <span className="font-mono text-slate-50 truncate">{user.username}</span>
                    </div>
                    <div className="font-mono text-xs text-dim mt-1 flex items-center gap-1">
                      <KeyRound className="size-3" />
                      {user.has_password ? 'password saved' : 'no password'}
                    </div>
                  </div>

                  {/* Удаление уносит с собой пароль из keychain и разлогинивает,
                      если удаляем текущую учётку, — поэтому в два шага. */}
                  {isConfirming ? (
                    <div className="flex items-center gap-2 shrink-0">
                      {/* Последствие называется до клика, а не после: удаление
                          текущей учётки рвёт подключение. */}
                      <span className="font-mono text-xs text-soft">
                        {user.id === activeUserId ? 'Delete and disconnect?' : 'Delete?'}
                      </span>
                      <Button
                        onClick={() => handleDelete(user)}
                        disabled={busy}
                        size="sm"
                        className="bg-danger text-slate-50 hover:bg-danger-hover font-mono disabled:opacity-50"
                      >
                        Delete
                      </Button>
                      <Button
                        onClick={() => setConfirmingId(null)}
                        variant="outline"
                        size="sm"
                        className="bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono"
                      >
                        Cancel
                      </Button>
                    </div>
                  ) : (
                    <div className="flex items-center gap-2 shrink-0">
                      <button
                        onClick={() => startEditing(user)}
                        className="p-1 text-dim hover:text-brand transition-colors cursor-pointer border-none bg-transparent outline-none"
                        title="Edit user"
                      >
                        <Pencil className="size-4" />
                      </button>
                      <button
                        onClick={() => setConfirmingId(user.id)}
                        className="p-1 text-dim hover:text-danger transition-colors cursor-pointer border-none bg-transparent outline-none"
                        title="Delete user"
                      >
                        <Trash2 className="size-4" />
                      </button>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>

        <div className="flex gap-3 pt-4 border-t border-edge flex-shrink-0">
          {onBack && (
            <Button
              onClick={onBack}
              variant="outline"
              className="bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono"
            >
              Back
            </Button>
          )}
          <Button
            onClick={() => onOpenChange(false)}
            variant="outline"
            className="ml-auto bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono"
          >
            Done
          </Button>
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}
