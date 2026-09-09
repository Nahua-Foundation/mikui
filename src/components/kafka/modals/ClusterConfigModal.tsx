import { useState, useEffect } from 'react';
import { Button } from '../../ui/button';
import { ArrowLeft, Loader2, Users } from 'lucide-react';
import { toast } from 'sonner';
import { Dialog, DialogHeader, DialogTitle, DialogDescription } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import { Checkbox } from '../../ui/checkbox';
import {
  ClusterConnectPayload,
  DEFAULT_SASL_MECHANISM,
  KafkaCluster,
  needsSasl,
  needsTls,
  newId,
  SASL_MECHANISMS,
  SchemaRegistryConfig,
} from '../types';
import * as api from '../api';
import { describeError } from '../api';
import { open as openDialog } from '@tauri-apps/plugin-dialog';

/** Имя файла из полного пути — показывать целиком незачем, он не помещается.
 *  Два разделителя, потому что путь приезжает и из Windows, и из POSIX. */
function fileName(path: string): string {
  return (path.split('\\').pop() || '').split('/').pop() || '';
}

/**
 * Поле «выбрать файл». Их в форме четыре, и ведут они себя одинаково: кнопка,
 * имя выбранного файла с полным путём в подсказке и «clear», чтобы выбор можно
 * было снять, а не только заменить другим.
 */
function FileField({
  label,
  value,
  onChange,
  title,
  extensions,
  hint,
}: {
  label: string;
  value: string;
  onChange: (path: string) => void;
  /** Заголовок системного диалога. */
  title: string;
  extensions: string[];
  hint?: string;
}) {
  return (
    <div className="space-y-1">
      <Label className="font-mono text-sm text-soft">{label}</Label>
      <div className="flex items-center gap-3">
        <Button
          type="button"
          variant="outline"
          className="bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono"
          onClick={async () => {
            const selected = await openDialog({
              title,
              multiple: false,
              filters: [
                { name: 'Certificates', extensions },
                { name: 'All Files', extensions: ['*'] },
              ],
            });
            if (typeof selected === 'string') onChange(selected);
          }}
        >
          {value ? 'Choose another file' : 'Choose file'}
        </Button>
        {value && (
          <>
            <div className="text-xs text-soft truncate max-w-[260px]" title={value}>
              Selected: <span className="text-slate-50">{fileName(value)}</span>
            </div>
            <button
              type="button"
              onClick={() => onChange('')}
              className="font-mono text-xs text-dim hover:text-slate-50 bg-transparent border-none cursor-pointer"
            >
              clear
            </button>
          </>
        )}
      </div>
      {hint && <p className="font-mono text-xs text-dim">{hint}</p>}
    </div>
  );
}

interface ClusterConfigModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  cluster?: KafkaCluster | null;
  mode: 'create' | 'edit';
  onBack?: () => void;
  onSaved?: (cluster: KafkaCluster) => void;
  /** Возвращённый список топиков форме не нужен — она только ждёт, чем
   *  кончилось подключение. Тип его не прячет: сузить `Promise<Topic[]>` до
   *  `Promise<void>` TypeScript не даёт, а заводить ради этого обёртку
   *  значило бы прятать, что за кнопкой стоит тот же самый `connect`. */
  onConnect?: (payload: ClusterConnectPayload, name: string) => Promise<unknown>;
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
  const [saslMechanism, setSaslMechanism] = useState<string>(DEFAULT_SASL_MECHANISM);

  // TLS. Клиентская пара — это mTLS; выключатели проверок нужны стендам и
  // кластерам, у которых имя в сертификате не совпадает с адресом.
  const [sslCertificatePath, setSslCertificatePath] = useState<string>('');
  const [sslKeyPath, setSslKeyPath] = useState<string>('');
  const [sslKeyPassword, setSslKeyPassword] = useState<string>('');
  /** Пароль ключа уже лежит в keychain: пустое поле означает «оставить». */
  const [sslHasKeyPassword, setSslHasKeyPassword] = useState<boolean>(false);
  const [skipHostnameCheck, setSkipHostnameCheck] = useState<boolean>(false);
  const [skipCertificateVerification, setSkipCertificateVerification] = useState<boolean>(false);

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

  /**
   * Что показать в выпадашке механизмов.
   *
   * Сохранённое значение показываем даже тогда, когда оно не поддержано: иначе
   * Select оставил бы триггер пустым, и человек не увидел бы, чем, по мнению
   * записи, он подключается. Такие значения остались от сборок, где в меню
   * были GSSAPI и OAUTHBEARER; бэкенд их теперь отвергает по имени, и заменить
   * их надо осознанно, а не молча.
   */
  const mechanismOptions = SASL_MECHANISMS.includes(saslMechanism)
    ? SASL_MECHANISMS
    : [...SASL_MECHANISMS, saslMechanism];

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
      // Умолчание то же, что и в бэкенде: запись без механизма подключается
      // именно им, и показать здесь что-то другое значило бы соврать.
      setSaslMechanism(cluster.sasl_mechanism || DEFAULT_SASL_MECHANISM);
      setSslCertificatePath(cluster.ssl_certificate_path || '');
      setSslKeyPath(cluster.ssl_key_path || '');
      setSslHasKeyPassword(cluster.has_key_password ?? false);
      setSslKeyPassword('');
      setSkipHostnameCheck(cluster.ssl_skip_hostname_check ?? false);
      setSkipCertificateVerification(cluster.ssl_skip_certificate_verification ?? false);
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
      setSaslMechanism(DEFAULT_SASL_MECHANISM);
      setSslCertificatePath('');
      setSslKeyPath('');
      setSslKeyPassword('');
      setSslHasKeyPassword(false);
      setSkipHostnameCheck(false);
      setSkipCertificateVerification(false);
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
    ssl_certificate_path: isSSLRequired ? sslCertificatePath : undefined,
    ssl_key_path: isSSLRequired ? sslKeyPath : undefined,
    // Как и пароль учётки: сохранённый бэкенд возьмёт из keychain сам, по id
    // кластера. Отправляем только введённый руками.
    ssl_key_password: isSSLRequired && sslKeyPassword ? sslKeyPassword : undefined,
    ssl_skip_hostname_check: isSSLRequired && skipHostnameCheck,
    ssl_skip_certificate_verification: isSSLRequired && skipCertificateVerification,
  });

  const buildConfig = (): KafkaCluster => ({
    id: cluster?.id ?? newId(),
    name: name.trim() || 'Untitled Cluster',
    brokers,
    security_protocol: securityProtocol,
    sasl_mechanism: isSASLRequired ? saslMechanism : undefined,
    ssl_ca_bundle_path: isSSLRequired ? sslCaBundlePath : undefined,
    ssl_certificate_path: isSSLRequired ? sslCertificatePath : undefined,
    ssl_key_path: isSSLRequired ? sslKeyPath : undefined,
    // Признак бэкенд пересчитает сам по тому, что сделал с keychain; здесь он
    // только чтобы тип был честным — как и список учёток ниже.
    has_key_password: sslHasKeyPassword,
    ssl_skip_hostname_check: isSSLRequired && skipHostnameCheck,
    ssl_skip_certificate_verification: isSSLRequired && skipCertificateVerification,
    created_at: cluster?.created_at || new Date().toISOString(),
    last_used: cluster?.last_used,
    users: cluster?.users ?? [],
    active_user_id: cluster?.active_user_id,
  });

  /**
   * Первая претензия к форме или `null`, если их нет.
   *
   * Проверяем ровно то, во что librdkafka упирается сама, но объясняет своими
   * словами и с задержкой: пустой адрес и половину клиентской пары она заметит
   * при создании клиента, а отсутствующий логин — уже отказом брокера. По одной
   * претензии за раз, потому что вторая обычно следствие первой.
   */
  const validate = (): string | null => {
    if (!brokers.trim()) return 'Bootstrap servers are required';

    if (isSASLRequired) {
      const login = editsCredentials ? username.trim() : (savedUser?.username ?? '');
      if (!login) return 'SASL needs a username — add a user for this cluster';
    }

    if (isSSLRequired) {
      const hasCertificate = sslCertificatePath.trim() !== '';
      const hasKey = sslKeyPath.trim() !== '';
      if (hasCertificate !== hasKey) {
        return 'Mutual TLS needs both the client certificate and the private key';
      }
    }

    return null;
  };

  const handleConnect = async () => {
    if (!onConnect) return;
    const complaint = validate();
    if (complaint) {
      toast.error(complaint);
      return;
    }
    try {
      setIsConnecting(true);
      await onConnect(buildPayload(), name || brokers);
      onOpenChange(false);
    } finally {
      setIsConnecting(false);
    }
  };

  const handleTestConnection = async () => {
    const complaint = validate();
    if (complaint) {
      toast.error(complaint);
      return;
    }
    try {
      setIsTesting(true);
      await api.clusterTest(buildPayload());
      toast.success('Connection test successful');
    } catch (e) {
      console.error(e);
      toast.error(`Connection test failed: ${describeError(e)}`);
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
    const complaint = validate();
    if (complaint) {
      toast.error(complaint);
      return;
    }
    try {
      setIsSaving(true);

      // Пустое поле пароля ключа означает «оставить сохранённый» — как и у
      // паролей учёток. Стереть его из keychain нужно ровно в одном случае:
      // сам ключ из настроек убрали, и пароль остался бы от несуществующего.
      const usesClientKey = isSSLRequired && sslKeyPath.trim() !== '';
      const keyPassword = sslKeyPassword
        ? sslKeyPassword
        : sslHasKeyPassword && !usesClientKey
          ? ''
          : undefined;

      let saved = await api.saveCluster(buildConfig(), keyPassword);

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

  // Через общие предикаты, а не своим сравнением: этот же вопрос задаёт шапка,
  // и два ответа на него разъехались бы на первом же новом протоколе.
  const isSSLRequired = needsTls(securityProtocol);
  const isSASLRequired = needsSasl(securityProtocol);

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
                      {mechanismOptions.map((mech) => (
                        <SelectItem
                          key={mech}
                          value={mech}
                          className="text-slate-50 font-mono focus:bg-edge"
                        >
                          {mech}
                          {!SASL_MECHANISMS.includes(mech) && ' · not supported'}
                        </SelectItem>
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

                <FileField
                  label="CA bundle"
                  value={sslCaBundlePath}
                  onChange={setSslCaBundlePath}
                  title="Select CA bundle file"
                  extensions={['crt', 'pem', 'cer']}
                  // Без своего файла корни берутся из системы: на macOS и Linux
                  // librdkafka прощупывает стандартные пути, на Windows читает
                  // Root store. Публично подписанному кластеру бандл не нужен.
                  hint="Only needed if the brokers are signed by a CA the system does not trust."
                />

                {/* Клиентская пара — mTLS. Именно PEM: keystore и truststore из
                    мира Java librdkafka не читает. */}
                <FileField
                  label="Client certificate"
                  value={sslCertificatePath}
                  onChange={(path) => {
                    setSslCertificatePath(path);
                    // Ключ без сертификата не значит ничего, а поля его после
                    // этого уже не видно: оставшийся путь запер бы форму
                    // жалобой на половину пары, которую нечем убрать.
                    if (!path) {
                      setSslKeyPath('');
                      setSslKeyPassword('');
                    }
                  }}
                  title="Select client certificate (PEM)"
                  extensions={['crt', 'pem', 'cer']}
                  hint="Only for clusters that ask the client to present a certificate."
                />

                {sslCertificatePath !== '' && (
                  <>
                    <FileField
                      label="Client private key"
                      value={sslKeyPath}
                      onChange={setSslKeyPath}
                      title="Select the private key (PEM)"
                      extensions={['key', 'pem']}
                    />
                    <div className="space-y-1">
                      <Label className="font-mono text-sm text-soft">Key password</Label>
                      <Input
                        type="password"
                        value={sslKeyPassword}
                        onChange={(e) => setSslKeyPassword(e.target.value)}
                        placeholder={
                          sslHasKeyPassword
                            ? 'Saved in keychain — leave blank to keep'
                            : 'Only if the key is encrypted'
                        }
                        className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
                      />
                    </div>
                  </>
                )}

                {/* Отказы от проверок. Оба выключателя существуют ради стендов
                    и кластеров, до которых иначе не дотянуться, поэтому названы
                    тем, что делают, и снабжены ценой. */}
                <div className="space-y-2 pt-1">
                  <div className="flex items-center gap-2">
                    <Checkbox
                      id="ssl-skip-hostname"
                      checked={skipHostnameCheck}
                      onCheckedChange={(checked) => setSkipHostnameCheck(checked === true)}
                    />
                    <Label
                      htmlFor="ssl-skip-hostname"
                      className="font-mono text-sm text-soft cursor-pointer"
                    >
                      Skip hostname verification
                    </Label>
                  </div>
                  <p className="font-mono text-xs text-dim">
                    For brokers reached by an address the certificate does not name — an IP or
                    an alias.
                  </p>

                  <div className="flex items-center gap-2 pt-1">
                    <Checkbox
                      id="ssl-skip-verification"
                      checked={skipCertificateVerification}
                      onCheckedChange={(checked) =>
                        setSkipCertificateVerification(checked === true)
                      }
                    />
                    <Label
                      htmlFor="ssl-skip-verification"
                      className="font-mono text-sm text-soft cursor-pointer"
                    >
                      Skip certificate verification
                    </Label>
                  </div>
                  <p className="font-mono text-xs text-dim">
                    Traffic stays encrypted, but nothing proves it is the right broker. For test
                    stands only.
                  </p>
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

                  {/* Без своего файла берётся системное хранилище — туда
                      корпоративный CA обычно и ставят централизованно. */}
                  <FileField
                    label="CA bundle"
                    value={registryCaPath}
                    onChange={setRegistryCaPath}
                    title="Select CA bundle for the schema registry"
                    extensions={['crt', 'pem', 'cer']}
                    hint="Only needed if the registry is signed by a CA the system does not trust."
                  />

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