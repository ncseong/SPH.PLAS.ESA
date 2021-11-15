using System.Diagnostics;
using System.Text;
using System;

using Serilog;

namespace SPH.PLAS.ESA
{
    public class StandardLog : IStandardLog
    {
        #region Constructors.
        public StandardLog(string filePath, string logLevel, bool isConsole)
        {
            logLevel = logLevel.ToLower();
            LoggerConfiguration loggerConfig = new LoggerConfiguration();
            loggerConfig.MinimumLevel.Verbose();
            if (logLevel.Contains("info"))
            {
                loggerConfig.MinimumLevel.Information();
            }
            else if (logLevel.Contains("debug"))
            {
                loggerConfig.MinimumLevel.Debug();
            }
            else if (logLevel.Contains("error"))
            {
                loggerConfig.MinimumLevel.Error();
            }
            else if (logLevel.Contains("warning"))
            {
                loggerConfig.MinimumLevel.Warning();
            }
            else if (logLevel.Contains("fatal"))
            {
                loggerConfig.MinimumLevel.Fatal();
            }
            if (isConsole)
            {
                loggerConfig.WriteTo.Console(outputTemplate: ConsoleTemplate);
            }
            loggerConfig.WriteTo.Async(w => w.File(filePath, shared: true, rollingInterval: RollingInterval.Day, rollOnFileSizeLimit: false, outputTemplate: AsynchTemplate));
            _logger = loggerConfig.CreateLogger();
        }
        public StandardLog(string fileName)
        {
            var sb = new StringBuilder(FileDirectory);
            sb.Append(fileName);
            FilePath = sb.ToString();

            _logger = new LoggerConfiguration()
                .MinimumLevel.Verbose()
                .Enrich.WithThreadId()
                .WriteTo.Console(outputTemplate: ConsoleTemplate)
                .WriteTo.Async(w => w.File(FilePath, shared: true, rollingInterval: RollingInterval.Hour, rollOnFileSizeLimit: true, fileSizeLimitBytes: 131072, outputTemplate: AsynchTemplate))
                .CreateLogger();
        }

        public StandardLog(string fileName, OnProgress onProgress, OnProgress onWriteLog) : this(fileName)
        {
            this._OnProgress = onProgress;
            this._OnWriteLog = onWriteLog;
        }
        #endregion

        #region Public Properties.
        public string FilePath { get; private set; }
        #endregion

        #region Interface Methods.
        public void Verbose(string message)
        {
            _logger.Verbose(message);
            WirteLog(message);
        }
        public void Verbose(string messageTemplate, params object[] propertyValues)
        {
            _logger.Verbose(messageTemplate, propertyValues);
            WirteLog(string.Format(messageTemplate, propertyValues));
        }
        public void Debug(string message)
        {
            _logger.Debug(message);
            WirteLog(message);
        }

        public void Debug(string messageTemplate, params object[] propertyValues)
        {
            _logger.Debug(messageTemplate, propertyValues);
            WirteLog(string.Format(messageTemplate, propertyValues));
        }
        public void Debug<T0, T1>(string messageTemplate, T0 propertyValue0, T1 propertyValue1)
        {
            _logger.Debug(messageTemplate, propertyValue0, propertyValue1);
        }
        public void Information(string message)
        {
            _logger.Information(message);
            WirteLog(message);
        }
        public void Information(string messageTemplate, params object[] propertyValues)
        {
            _logger.Information(messageTemplate, propertyValues);
            WirteLog(string.Format(messageTemplate, propertyValues));
        }
        public void Information<T0, T1>(string messageTemplate, T0 propertyValue0, T1 propertyValue1)
        {
            _logger.Information(messageTemplate, propertyValue0, propertyValue1);
        }
        public void Warning(string message)
        {
            _logger.Warning(message);
            WirteLog(message);
        }
        public void Warning(string messageTemplate, params object[] propertyValues)
        {
            _logger.Warning(messageTemplate, propertyValues);
            WirteLog(string.Format(messageTemplate, propertyValues));
        }
        public void Warning<T0, T1>(string messageTemplate, T0 propertyValue0, T1 propertyValue1)
        {
            StackFrame fr = new StackFrame(1, true);
            _logger.Warning(string.Format(DetailFormat, messageTemplate, fr.GetFileLineNumber(), fr.GetMethod(), fr.GetMethod().DeclaringType.FullName),
                propertyValue0, propertyValue1);
        }
        public void Error(string message)
        {
            StackFrame fr = new StackFrame(1, true);
            _logger.Error(string.Format(DetailFormat, message, fr.GetFileLineNumber(), fr.GetMethod(), fr.GetMethod().DeclaringType.FullName));
            WirteLog(message);
        }
        public void Error(Exception e, string message)
        {
            _logger.Error(e, message);
        }
        public void Error(string messageTemplate, params object[] propertyValues)
        {
            _logger.Error(messageTemplate, propertyValues);
            WirteLog(string.Format(messageTemplate, propertyValues));
        }
        public void Error<T0, T1>(string messageTemplate, T0 propertyValue0, T1 propertyValue1)
        {
            StackFrame fr = new StackFrame(1, true);
            _logger.Error(string.Format(DetailFormat, messageTemplate, fr.GetFileLineNumber(), fr.GetMethod(), fr.GetMethod().DeclaringType.FullName),
                propertyValue0, propertyValue1);
        }
        public void Fatal(string message)
        {
            StackFrame fr = new StackFrame(1, true);
            _logger.Fatal(string.Format(DetailFormat, message, fr.GetFileLineNumber(), fr.GetMethod(), fr.GetMethod().DeclaringType.FullName));
            WirteLog(message);
        }
        public void Fatal(string messageTemplate, params object[] propertyValues)
        {
            _logger.Fatal(messageTemplate, propertyValues);
            WirteLog(string.Format(messageTemplate, propertyValues));
        }
        public void Fatal(Exception e, string msg)
        {
            _logger.Fatal(e, msg);
        }
        public void Fatal<T0, T1>(string message, T0 propertyValue0, T1 propertyValue1)
        {
            StackFrame fr = new StackFrame(1, true);
            _logger.Fatal(string.Format(DetailFormat, message, fr.GetFileLineNumber(), fr.GetMethod(), fr.GetMethod().DeclaringType.FullName),
                propertyValue0, propertyValue1);
        }
        public void WirteMsg(string message)
        {
            if (this._OnProgress != null)
            {
                this._OnProgress(message);
            }
        }
        public void WirteLog(string message)
        {
            if (this._OnWriteLog != null)
            {
                this._OnWriteLog(message);
            }
        }
        public void Exit()
        {
            Log.CloseAndFlush();
        }
        #endregion

        private OnProgress _OnProgress { get; set; } = null;
        private OnProgress _OnWriteLog { get; set; } = null;
        public delegate void OnProgress(string msg);

        #region Private Members.
        private ILogger _logger;
        #endregion

        #region Format. for Strings.
        private static readonly string AsynchTemplate = "{Timestamp:HH:mm:ss.fff} [{Level:u3}] {Message:lj}{NewLine}{Exception}";
        private static readonly string ConsoleTemplate = "[{Timestamp:HH:mm:ss} {Level:u3} {ThreadId}] {SourceContext}.{Method} {Message:lj} {NewLine}{Exception}";
        private static readonly string FileDirectory = @".\logs\";
        private static readonly string DetailFormat = "{0} L:{1} M:{2} O:{3}";
        #endregion
    }
}