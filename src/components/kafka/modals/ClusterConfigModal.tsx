import { useState, useEffect } from 'react';
import { Button } from '../../ui/button';
import { ArrowLeft, Loader2, Users } from 'lucide-react';
import { toast } from 'sonner';
import { Dialog, DialogHeader, DialogTitle, DialogDescription } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import { ClusterConnectPayload, KafkaCluster, newId, SchemaRegistryConfig } from '../types';
import * as api from '../api';
import { describeError } from '../api';
import { open as openDialog } from '@tauri-apps/plugin-dialog';

/** Запасной список на случай, если бэкенд не ответил. GSSAPI сюда не входит:
 *  он есть не в каждой сборке (Cyrus SASL линкуется только фичей `gssapi`). */
const FALLBACK_MECHANISMS = ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512', 'OAUTHBEARER'];

interface ClusterConfigModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  cluster?: KafkaCluster | null;
  mode: 'create' | 'edit';
  onBack?: () => void;
  onSaved?: (cluster: KafkaCluster) => void;
  onConnect?: (payload: ClusterConnectPayload, name: string) => Promise<void>;
  /** Открыть список Kafka-пользователей этого кластера. */
  onManageUsers?: (cluster: KafkaCluster) => void;
  /** Подключены ли мы прямо сейчас именно к этому кластеру. */
  isConnected?: boolean;
  /** Применить сохранённые настройки — переподключиться с ними. */
  onApply?: (cluster: KafkaCluster) => Promise<void>;
}

export function ClusterConfigModal({
  open,
  onOpenChange,
  cluster,
  mode,
  onBack,
  onSaved,
  onConnect,
  onManageUsers,
  isConnected = false,
  onApply,
}: ClusterConfigModalProps) {
  const [name, setName] = useState<string>('');
  const [brokers, setBrokers] = useState<string>('localhost:9092');
  const [securityProtocol, setSecurityProtocol] = useState<string>('PLAINTEXT');
  /** Какую учётку правит форма. null — новую, ещё не заведённую. */
  const [userId, setUserId] = useState<string | null>(null);
  const [username, setUsername] = useState<string>('');
  const [password, setPassword] = useState<string>('');
  const [sslCaBundlePath, setSslCaBundlePath] = useState<string>('');
  const [saslMechanism, setSaslMechanism] = useState<string>('PLAIN');

  // Schema Registry. Живёт на кластере, а не на топике: реестр в кластере один.
  const [registryUrl, setRegistryUrl] = useState<string>('');
  const [registryUser, setRegistryUser] = useState<string>('');
  const [registryPassword, setRegistryPassword] = useState<string>('');
  const [registryCaPath, setRegistryCaPath] = useState<string>('');
  /** Пароль реестра уже лежит в keychain. Пустое поле тогда означает
   *  «оставить», а не «стереть», — как и у пароля учётки. */
  const [registryHasPassword, setRegistryHasPassword] = useState<boolean>(false);

  const [isConnecting, setIsConnecting] = useState<boolean>(false);
  const [isTesting, setIsTesting] = useState<boolean>(false);
  const [isTestingRegistry, setIsTestingRegistry] = useState<boolean>(false);
  const [isSaving, setIsSaving] = useState<boolean>(false);
  const [mechanisms, setMechanisms] = useState<string[]>(FALLBACK_MECHANISMS);

  // Какие механизмы доступны, знает только бэкенд: GSSAPI требует Cyrus SASL,
  // который линкуется не во всех сборках. Спрашиваем один раз за жизнь модалки.
  useEffect(() => {
    api
      .saslMechanisms()
      .then(setMechanisms)
      .catch((e) => console.error('Failed to load SASL mechanisms', e));
  }, []);

  /**
   * Правит ли форма логин и пароль.
   *
   * Пока к кластеру не подключены — правит: сохранённая учётка с неверным
   * паролем иначе запирает вход намертво. Подключиться нельзя, а выбрать
   * другую учётку можно только уже подключившись — селектор пользователя
   * живёт в шапке и до подключения его нет.
   *
   * Когда подключены — не правит: там учётками заведует Manage users, и
   * держать вторую точку редактирования тех же кредов значило бы менять их
   * из двух мест с разным результатом.
   */
  const editsCredentials = mode === 'create' || !isConnected;

  // Load cluster data when editing
  useEffect(() => {
    if (cluster && mode === 'edit') {
      setName(cluster.name);
      setBrokers(cluster.brokers);
      setSecurityProtocol(cluster.security_protocol);
      setSslCaBundlePath(cluster.ssl_ca_bundle_path || '');
      setSaslMechanism(cluster.sasl_mechanism || 'PLAIN');
      const user = api.activeUser(cluster);
      setUserId(user?.id ?? null);
      setUsername(user?.username ?? '');
      // Пароль из keychain сюда не тянем: форме он не нужен и незачем гонять
      // его через IPC. Пустое поле означает «оставить как есть».
      setPassword('');
      setRegistryUrl(cluster.schema_registry?.url ?? '');
      setRegistryUser(cluster.schema_registry?.username ?? '');
      setRegistryCaPath(cluster.schema_registry?.ssl_ca_bundle_path ?? '');
      setRegistryHasPassword(cluster.schema_registry?.has_password ?? false);
      setRegistryPassword('');
    } else {
      // Reset form for create mode
      setName('');
      setBrokers('localhost:9092');
      setSecurityProtocol('PLAINTEXT');
      setUserId(null);
      setUsername('');
      setPassword('');
      setSslCaBundlePath('');
      setSaslMechanism('PLAIN');
      setRegistryUrl('');
      setRegistryUser('');
      setRegistryPassword('');
      setRegistryCaPath('');
      setRegistryHasPassword(false);
    }
  }, [cluster, mode, open]);

  const users = cluster?.users ?? [];
  /** Учётка, которую правит форма, — в том виде, в каком она сохранена. */
  const savedUser = users.find((u) => u.id === userId) ?? null;

  /** Переключение на другую сохранённую учётку в форме. */
  const selectUser = (id: string) => {
    const user = users.find((u) => u.id === id);
    if (!user) return;
    setUserId(user.id);
    setUsername(user.username);
    setPassword('');
  };

  const buildPayload = (): ClusterConnectPayload => ({
    id: cluster?.id,
    // Пароль сохранённой учётки не покидает бэкенд — уезжает только её id.
    // Но только если пользователь не ввёл в форме новый: введённый главнее.
    user_id: password ? undefined : savedUser?.id,
    brokers,
    security_protocol: securityProtocol,
    sasl_mechanism: isSASLRequired ? saslMechanism : undefined,
    username: isSASLRequired ? (editsCredentials ? username : savedUser?.username) : undefined,
    password: isSASLRequired && password ? password : undefined,
    ssl_ca_bundle_path: isSSLRequired ? sslCaBundlePath : undefined,
  });

  const buildConfig = (): KafkaCluster => ({
    id: cluster?.id ?? newId(),
    name: name.trim() || 'Untitled Cluster',
    brokers,
    security_protocol: securityProtocol,
    sasl_mechanism: isSASLRequired ? saslMechanism : undefined,
    ssl_ca_bundle_path: isSSLRequired ? sslCaBundlePath : undefined,
    created_at: cluster?.created_at || new Date().toISOString(),
    last_used: cluster?.last_used,
    // Список учёток бэкенд всё равно возьмёт из своей записи — здесь он только
    // чтобы тип был честным.
    users: cluster?.users ?? [],
    active_user_id: cluster?.active_user_id,
  });

  const handleConnect = async () => {
    if (!onConnect) return;
    try {
      setIsConnecting(true);
      await onConnect(buildPayload(), name || brokers);
      onOpenChange(false);
    } finally {
      setIsConnecting(false);
    }
  };

  const handleTestConnection = async () => {
    try {
      setIsTesting(true);
      await api.clusterTest(buildPayload());
      toast.success('Connection test successful');
    } catch (e) {
      console.error(e);
      toast.error(`Connection test failed: ${e}`);
    } finally {
      setIsTesting(false);
    }
  };

  /** Настройки реестра в том виде, в каком их набрали. */
  const buildRegistry = (): SchemaRegistryConfig => ({
    url: registryUrl.trim(),
    username: registryUser.trim() || null,
    has_password: registryHasPassword,
    ssl_ca_bundle_path: registryCaPath || null,
  });

  /**
   * Проверяет реестр, ничего не сохраняя.
   *
   * Отдельная кнопка рядом с полем, а не часть общей проверки подключения:
   * реестр — это другой хост, другая авторизация и другой TLS, и «подключение
   * работает, а схемы не читаются» — совершенно обычный расклад, который надо
   * уметь отличать.
   */
  const handleTestRegistry = async () => {
    try {
      setIsTestingRegistry(true);
      const subjects = await api.testSchemaRegistry(
        cluster?.id ?? null,
        buildRegistry(),
        // Пустое поле — «взять сохранённый», если он есть.
        registryPassword || undefined,
      );
      toast.success(`Schema registry answered: ${subjects} subject${subjects === 1 ? '' : 's'}`);
    } catch (e) {
      console.error('Schema registry test failed', e);
      toast.error(`Schema registry test failed: ${describeError(e)}`);
    } finally {
      setIsTestingRegistry(false);
    }
  };

  const handleSave = async () => {
    try {
      setIsSaving(true);
      let saved = await api.saveCluster(buildConfig());

      // Логин из формы — это учётка кластера, а не отдельная настройка
      // подключения: при создании она заводится первой, при правке обновляет
      // выбранную. Без этого шага новый SASL-кластер оставался бы без единого
      // логина, и подключаться к нему было бы не под кем.
      const login = username.trim();
      if (isSASLRequired && editsCredentials && login) {
        saved = await api.saveClusterUser(
          saved.id,
          { id: userId ?? newId(), username: login, has_password: false },
          // Пустое поле означает «оставить сохранённый пароль», а не «стереть».
          password ? password : undefined,
          // Выбранная в форме учётка становится текущей — и переживает
          // неудачную попытку подключиться, иначе выбор пришлось бы делать
          // заново после каждой опечатки в пароле.
          true,
        );
      }

      // Реестр — отдельной командой, и только после сохранения кластера: до
      // него у новой записи ещё нет id, а именно им ключуется пароль в keychain.
      const url = registryUrl.trim();
      if (url) {
        saved = await api.saveSchemaRegistry(
          saved.id,
          buildRegistry(),
          registryPassword ? registryPassword : undefined,
        );
      } else if (cluster?.schema_registry) {
        // Поле очистили — это «убрать реестр», а не «оставить как было».
        saved = await api.deleteSchemaRegistry(saved.id);
      }

      onSaved?.(saved);
      toast.success(mode === 'create' ? 'Cluster created' : 'Cluster updated');
      onOpenChange(false);

      // Настройки сохранены — применяем их. Ошибку подключения показывает сам
      // `connect`, и превращать её здесь во второй тост про неудачное
      // сохранение нельзя: сохранение как раз удалось.
      onApply?.(saved).catch(() => {});
    } catch (e) {
      console.error(e);
      toast.error(`Failed to save cluster: ${e}`);
    } finally {
      setIsSaving(false);
    }
  };

  const handleBack = () => {
    if (onBack) {
      onBack();
    }
  };

  const isSSLRequired = securityProtocol === 'SSL' || securityProtocol === 'SASL_SSL';
  const isSASLRequired = securityProtocol === 'SASL_PLAINTEXT' || securityProtocol === 'SASL_SSL';

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContentNoClose className="max-w-2xl max-h-[90vh] bg-surface border-edge text-slate-50 flex flex-col">
        <DialogHeader className="border-b border-edge pb-4 flex-shrink-0">
          <div className="flex items-center gap-3">
            {onBack && (
              <Button
                onClick={handleBack}
                variant="ghost"
                size="sm"
                className="text-soft hover:text-brand hover:bg-edge p-2"
              >
                <ArrowLeft className="size-4" />
              </Button>
            )}
            <div>
              <DialogTitle className="font-mono text-soft text-lg">
                {mode === 'create' ? 'Create New Cluster' : 'Edit Cluster'}
              </DialogTitle>
              <DialogDescription className="text-sm text-soft">
                Configure your Kafka cluster connection settings
              </DialogDescription>
            </div>
          </div>
        </DialogHeader>
        
        <div className="flex-1 overflow-auto">
          <div className="space-y-6 p-1 pr-3">
            {/* Cluster Name */}
            <div className="space-y-2">
              <Label className="font-mono text-sm text-soft">
                Cluster Name
              </Label>
              <Input
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="My Kafka Cluster"
                className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
              />
            </div>

            {/* Brokers */}
            <div className="space-y-2">
              <Label className="font-mono text-sm text-soft">
                Bootstrap Servers
              </Label>
              <Input
                value={brokers}
                onChange={(e) => setBrokers(e.target.value)}
                placeholder="localhost:9092"
                className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
              />
            </div>

            {/* Security Protocol */}
            <div className="space-y-2">
              <Label className="font-mono text-sm text-soft">
                Security Protocol
              </Label>
              <Select value={securityProtocol} onValueChange={setSecurityProtocol}>
                <SelectTrigger className="bg-surface border-edge text-slate-50 font-mono">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent className="bg-surface border-edge">
                  <SelectItem value="PLAINTEXT" className="text-slate-50 font-mono focus:bg-edge">PLAINTEXT</SelectItem>
                  <SelectItem value="SSL" className="text-slate-50 font-mono focus:bg-edge">SSL</SelectItem>
                  <SelectItem value="SASL_PLAINTEXT" className="text-slate-50 font-mono focus:bg-edge">SASL_PLAINTEXT</SelectItem>
                  <SelectItem value="SASL_SSL" className="text-slate-50 font-mono focus:bg-edge">SASL_SSL</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {/* SASL Configuration */}
            {isSASLRequired && (
              <div className="space-y-4 border border-edge rounded-lg p-4">
                <h3 className="font-mono text-brand text-sm">SASL Configuration</h3>
                
                <div className="space-y-2">
                  <Label className="font-mono text-sm text-soft">
                    SASL Mechanism
                  </Label>
                  <Select value={saslMechanism} onValueChange={setSaslMechanism}>
                    <SelectTrigger className="bg-surface border-edge text-slate-50 font-mono">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent className="bg-surface border-edge">
                      {mechanisms.map((mech) => (
                        <SelectItem key={mech} value={mech} className="text-slate-50 font-mono focus:bg-edge">{mech}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                {/* Пока подключены — логинами заведует Manage users: они там
                    списком, и переключаться между ними надо на лету. Пока не
                    подключены — правим прямо здесь, иначе учётка с неверным
                    паролем запирает вход: подключиться нельзя, а селектор
                    пользователя до подключения не существует. */}
                {cluster && mode === 'edit' && !editsCredentials ? (
                  <div className="space-y-2">
                    <Label className="font-mono text-sm text-soft">Users</Label>
                    <div className="flex items-center justify-between gap-3">
                      <div className="font-mono text-sm text-slate-50 truncate">
                        {cluster.users.length > 0 ? (
                          cluster.users.map((u) => u.username).join(', ')
                        ) : (
                          <span className="text-dim">No users yet</span>
                        )}
                      </div>
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        onClick={() => onManageUsers?.(cluster)}
                        className="shrink-0 bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono"
                      >
                        <Users className="size-4 mr-2" />
                        Manage users
                      </Button>
                    </div>
                  </div>
                ) : (
                  <>
                    {/* Переключение между уже заведёнными учётками. Без него
                        форма умела бы только ПЕРЕИМЕНОВАТЬ текущую: набрать в
                        поле имя соседнего пользователя значило бы затереть им
                        запись того, кто выбран, а не выбрать соседа. */}
                    {users.length > 1 && (
                      <div className="space-y-2">
                        <Label className="font-mono text-sm text-soft">User</Label>
                        <Select value={userId ?? ''} onValueChange={selectUser}>
                          <SelectTrigger className="bg-surface border-edge text-slate-50 font-mono">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent className="bg-surface border-edge">
                            {users.map((user) => (
                              <SelectItem
                                key={user.id}
                                value={user.id}
                                className="text-slate-50 font-mono focus:bg-edge"
                              >
                                {user.username}
                                {!user.has_password && ' · no password'}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                    )}
                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <Label className="font-mono text-sm text-soft">Username</Label>
                        <Input
                          value={username}
                          onChange={(e) => setUsername(e.target.value)}
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
                          placeholder={
                            savedUser?.has_password
                              ? 'Saved in keychain — leave blank to keep'
                              : 'Enter password'
                          }
                          className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
                        />
                      </div>
                    </div>
                  </>
                )}
                <p className="font-mono text-xs text-dim">
                  {mode === 'create'
                    ? "Saved as the cluster's first user — add more from the header once connected."
                    : editsCredentials
                      ? 'Add or remove users from the header once connected.'
                      : 'Connected right now — credentials are managed from Manage users.'}
                </p>
              </div>
            )}

            {/* SSL Configuration */}
            {isSSLRequired && (
              <div className="space-y-4 border border-edge rounded-lg p-4">
                <h3 className="font-mono text-brand text-sm">SSL Configuration</h3>

                <div className="grid grid-cols-1 gap-1">
                  <div className="space-y-1">
                    <Label className="font-mono text-sm text-soft">
                      CA bundle
                    </Label>
                    <div className="flex items-center gap-3">
                      <Button
                        type="button"
                        variant="outline"
                        className="bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono"
                        onClick={async () => {
                          const selected = await openDialog({
                            title: 'Select CA bundle file',
                            multiple: false,
                            filters: [
                              { name: 'Certificates', extensions: ['crt', 'pem', 'cer'] },
                              { name: 'All Files', extensions: ['*'] },
                            ],
                          });
                          if (typeof selected === 'string') {
                            setSslCaBundlePath(selected);
                          }
                        }}
                      >
                        {sslCaBundlePath ? 'Choose another file' : 'Choose file'}
                      </Button>
                      {sslCaBundlePath && (
                        <div className="text-xs text-soft truncate max-w-[260px]" title={sslCaBundlePath}>
                          Selected: <span className="text-slate-50">{(sslCaBundlePath.split('\\').pop() || '').split('/').pop()}</span>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            )}

            {/* Schema Registry.
                Не в настройках топика, где раньше стояла заглушка: реестр в
                кластере один, и вводить его для каждого топика заново значило
                бы переписывать одно и то же по десять раз. Топику остаётся
                выбор subject. */}
            <div className="space-y-4 border border-edge rounded-lg p-4">
              <div className="flex items-center justify-between">
                <h3 className="font-mono text-brand text-sm">Schema Registry</h3>
                <span className="font-mono text-xs text-dim">for Avro topics</span>
              </div>

              <div className="space-y-1">
                <Label className="font-mono text-sm text-soft">URL</Label>
                <Input
                  value={registryUrl}
                  onChange={(e) => setRegistryUrl(e.target.value)}
                  placeholder="http://localhost:8081"
                  className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
                />
                <p className="font-mono text-xs text-dim">
                  Leave empty if there is none — Avro topics can also be read from local .avsc
                  files.
                </p>
              </div>

              {registryUrl.trim() !== '' && (
                <>
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-1">
                      <Label className="font-mono text-sm text-soft">Username</Label>
                      <Input
                        value={registryUser}
                        onChange={(e) => setRegistryUser(e.target.value)}
                        placeholder="Optional"
                        className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
                      />
                    </div>
                    <div className="space-y-1">
                      <Label className="font-mono text-sm text-soft">Password</Label>
                      <Input
                        type="password"
                        value={registryPassword}
                        onChange={(e) => setRegistryPassword(e.target.value)}
                        placeholder={registryHasPassword ? 'Saved — leave empty to keep' : 'Optional'}
                        className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
                      />
                    </div>
                  </div>

                  <div className="space-y-1">
                    <Label className="font-mono text-sm text-soft">CA bundle</Label>
                    <div className="flex items-center gap-3">
                      <Button
                        type="button"
                        variant="outline"
                        className="bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono"
                        onClick={async () => {
                          const selected = await openDialog({
                            title: 'Select CA bundle for the schema registry',
                            multiple: false,
                            filters: [
                              { name: 'Certificates', extensions: ['crt', 'pem', 'cer'] },
                              { name: 'All Files', extensions: ['*'] },
                            ],
                          });
                          if (typeof selected === 'string') setRegistryCaPath(selected);
                        }}
                      >
                        {registryCaPath ? 'Choose another file' : 'Choose file'}
                      </Button>
                      {registryCaPath && (
                        <div
                          className="text-xs text-soft truncate max-w-[260px]"
                          title={registryCaPath}
                        >
                          Selected:{' '}
                          <span className="text-slate-50">
                            {(registryCaPath.split('\\').pop() || '').split('/').pop()}
                          </span>
                        </div>
                      )}
                      {registryCaPath && (
                        <button
                          type="button"
                          onClick={() => setRegistryCaPath('')}
                          className="font-mono text-xs text-dim hover:text-slate-50 bg-transparent border-none cursor-pointer"
                        >
                          clear
                        </button>
                      )}
                    </div>
                    {/* Без своего файла берётся системное хранилище — туда
                        корпоративный CA обычно и ставят централизованно. */}
                    <p className="font-mono text-xs text-dim">
                      Only needed if the registry is signed by a CA the system does not trust.
                    </p>
                  </div>

                  <Button
                    type="button"
                    onClick={handleTestRegistry}
                    variant="outline"
                    disabled={isTestingRegistry}
                    className="bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono disabled:opacity-50"
                  >
                    {isTestingRegistry && <Loader2 className="size-4 animate-spin" />}
                    {isTestingRegistry ? 'Testing…' : 'Test registry'}
                  </Button>
                </>
              )}
            </div>
          </div>
        </div>

        {/* Action Buttons */}
        {/* Ничего не записывается до нажатия Save, поэтому «отменить» — это
            просто закрыть форму. Кнопка называется Cancel, а не Back: она
            отвечает на вопрос «что будет с моими правками», а не «куда я
            попаду». Вернуться к списку кластеров можно и стрелкой в шапке. */}
        <div className="flex gap-3 pt-4 border-t border-edge flex-shrink-0">
          {onBack && (
            <Button
              onClick={handleBack}
              variant="outline"
              disabled={isSaving}
              className="bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono"
            >
              Cancel
            </Button>
          )}
          <Button
            onClick={handleTestConnection}
            variant="outline"
            disabled={isTesting || isConnecting || isSaving}
            className="flex-1 bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {isTesting && <Loader2 className="size-4 animate-spin" />}
            {isTesting ? 'Testing…' : 'Test'}
          </Button>
          {/* В режиме правки Save сам применяет настройки, и отдельный Connect
              рядом с ним делал бы почти то же самое — только не сохраняя. */}
          {mode === 'create' && (
            <Button
              onClick={handleSave}
              variant="outline"
              disabled={isConnecting || isSaving}
              className="flex-1 bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Save
            </Button>
          )}
          {mode === 'create' ? (
            <Button
              onClick={handleConnect}
              disabled={isConnecting || isTesting || isSaving}
              className="flex-1 bg-brand text-surface hover:bg-brand-hover font-mono disabled:opacity-70 disabled:cursor-not-allowed"
            >
              {isConnecting && <Loader2 className="size-4 animate-spin" />}
              {isConnecting ? 'Connecting…' : 'Connect'}
            </Button>
          ) : (
            <Button
              onClick={handleSave}
              disabled={isSaving || isTesting}
              className="flex-1 bg-brand text-surface hover:bg-brand-hover font-mono disabled:opacity-70 disabled:cursor-not-allowed"
            >
              {isSaving && <Loader2 className="size-4 animate-spin" />}
              {isSaving ? 'Saving…' : 'Save & connect'}
            </Button>
          )}
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}