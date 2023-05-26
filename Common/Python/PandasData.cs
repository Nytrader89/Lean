/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using Python.Runtime;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Util;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace QuantConnect.Python
{
    /// <summary>
    /// Organizes a list of data to create pandas.DataFrames
    /// </summary>
    public class PandasData
    {
        private const string Open = "open";
        private const string High = "high";
        private const string Low = "low";
        private const string Close = "close";
        private const string Volume = "volume";

        private const string AskOpen = "askopen";
        private const string AskHigh = "askhigh";
        private const string AskLow = "asklow";
        private const string AskClose = "askclose";
        private const string AskPrice = "askprice";
        private const string AskSize = "asksize";

        private const string BidOpen = "bidopen";
        private const string BidHigh = "bidhigh";
        private const string BidLow = "bidlow";
        private const string BidClose = "bidclose";
        private const string BidPrice = "bidprice";
        private const string BidSize = "bidsize";

        private const string LastPrice = "lastprice";
        private const string Quantity = "quantity";
        private const string Exchange = "exchange";
        private const string Suspicious = "suspicious";
        private const string OpenInterest = "openinterest";

        // we keep these so we don't need to ask for them each time
        private static PyString _empty;
        private static PyObject _pandas;
        private static PyObject _seriesFactory;
        private static PyObject _dataFrameFactory;
        private static PyObject _multiIndexFactory;

        private static PyList _defaultNames;
        private static PyList _level2Names;
        private static PyList _level3Names;

        private readonly static HashSet<string> _baseDataProperties = typeof(BaseData).GetProperties().ToHashSet(x => x.Name.ToLowerInvariant());
        private readonly static ConcurrentDictionary<Type, IEnumerable<MemberInfo>> _membersByType = new ();
        private readonly static IReadOnlyList<string> _standardColumns = new string []
        {
                Open,    High,    Low,    Close, LastPrice,  Volume,
            AskOpen, AskHigh, AskLow, AskClose,  AskPrice, AskSize, Quantity, Suspicious,
            BidOpen, BidHigh, BidLow, BidClose,  BidPrice, BidSize, Exchange, OpenInterest
        };

        private readonly Symbol _symbol;
        private Serie _open;
        private Serie _high;
        private Serie _low;
        private Serie _close;
        private Serie _volume;

        private Serie _askopen;
        private Serie _askhigh;
        private Serie _asklow;
        private Serie _askclose;
        private Serie _askprice;
        private Serie _asksize;

        private Serie _bidopen;
        private Serie _bidhigh;
        private Serie _bidlow;
        private Serie _bidclose;
        private Serie _bidprice;
        private Serie _bidsize;

        private Serie _lastPrice;
        private Serie _exchange;
        private Serie _quantity;
        private Serie _suspicious;
        private Serie _openInterest;

        private readonly Dictionary<string, Serie> _series;

        private readonly IEnumerable<MemberInfo> _members = Enumerable.Empty<MemberInfo>();

        /// <summary>
        /// Gets true if this is a custom data request, false for normal QC data
        /// </summary>
        public bool IsCustomData { get; }

        /// <summary>
        /// Implied levels of a multi index pandas.Series (depends on the security type)
        /// </summary>
        public int Levels { get; } = 2;

        /// <summary>
        /// Initializes an instance of <see cref="PandasData"/>
        /// </summary>
        public PandasData(object data)
        {
            if (_pandas == null)
            {
                using (Py.GIL())
                {
                    // Use our PandasMapper class that modifies pandas indexing to support tickers, symbols and SIDs
                    _pandas = Py.Import("PandasMapper");
                    _seriesFactory = _pandas.GetAttr("Series");
                    _dataFrameFactory = _pandas.GetAttr("DataFrame");
                    using var multiIndex = _pandas.GetAttr("MultiIndex");
                    _multiIndexFactory = multiIndex.GetAttr("from_tuples");
                    _empty = new PyString(string.Empty);

                    var time = new PyString("time");
                    var symbol = new PyString("symbol");
                    var expiry = new PyString("expiry");
                    _defaultNames = new PyList(new PyObject[] { expiry, new PyString("strike"), new PyString("type"), symbol, time });
                    _level2Names = new PyList(new PyObject[] { symbol, time });
                    _level3Names = new PyList(new PyObject[] { expiry, symbol, time });
                }
            }

            // in the case we get a list/collection of data we take the first data point to determine the type
            // but it's also possible to get a data which supports enumerating we don't care about those cases
            if (data is not IBaseData && data is IEnumerable enumerable)
            {
                foreach (var item in enumerable)
                {
                    data = item;
                    break;
                }
            }

            var type = data.GetType();
            IsCustomData = type.Namespace != typeof(Bar).Namespace;
            _symbol = ((IBaseData)data).Symbol;

            if (_symbol.SecurityType == SecurityType.Future) Levels = 3;
            if (_symbol.SecurityType.IsOption()) Levels = 5;

            IEnumerable<string> columns = _standardColumns;

            if (IsCustomData)
            {
                var keys = (data as DynamicData)?.GetStorageDictionary().ToHashSet(x => x.Key);

                // C# types that are not DynamicData type
                if (keys == null)
                {
                    if (_membersByType.TryGetValue(type, out _members))
                    {
                        keys = _members.ToHashSet(x => x.Name.ToLowerInvariant());
                    }
                    else
                    {
                        var members = type.GetMembers().Where(x => x.MemberType == MemberTypes.Field || x.MemberType == MemberTypes.Property).ToList();

                        var duplicateKeys = members.GroupBy(x => x.Name.ToLowerInvariant()).Where(x => x.Count() > 1).Select(x => x.Key);
                        foreach (var duplicateKey in duplicateKeys)
                        {
                            throw new ArgumentException($"PandasData.ctor(): {Messages.PandasData.DuplicateKey(duplicateKey, type.FullName)}");
                        }

                        // If the custom data derives from a Market Data (e.g. Tick, TradeBar, QuoteBar), exclude its keys
                        keys = members.ToHashSet(x => x.Name.ToLowerInvariant());
                        keys.ExceptWith(_baseDataProperties);
                        keys.ExceptWith(GetPropertiesNames(typeof(QuoteBar), type));
                        keys.ExceptWith(GetPropertiesNames(typeof(TradeBar), type));
                        keys.ExceptWith(GetPropertiesNames(typeof(Tick), type));
                        keys.Add("value");

                        _members = members.Where(x => keys.Contains(x.Name.ToLowerInvariant())).ToList();
                        _membersByType.TryAdd(type, _members);
                    }
                }

                var customColumns = new HashSet<string>(columns);
                customColumns.Add("value");
                customColumns.UnionWith(keys);

                columns = customColumns;
            }

            _series = columns.ToDictionary(k => k, v => new Serie());
        }

        /// <summary>
        /// Adds security data object to the end of the lists
        /// </summary>
        /// <param name="baseData"><see cref="IBaseData"/> object that contains security data</param>
        public void Add(object baseData)
        {
            foreach (var member in _members)
            {
                var key = member.Name.ToLowerInvariant();
                var endTime = ((IBaseData)baseData).EndTime;
                var propertyMember = member as PropertyInfo;
                if (propertyMember != null)
                {
                    AddToSeries(key, endTime, propertyMember.GetValue(baseData));
                    continue;
                }
                var fieldMember = member as FieldInfo;
                if (fieldMember != null)
                {
                    AddToSeries(key, endTime, fieldMember.GetValue(baseData));
                }
            }

            var storage = (baseData as DynamicData)?.GetStorageDictionary();
            if (storage != null)
            {
                var endTime = ((IBaseData) baseData).EndTime;
                var value = ((IBaseData) baseData).Value;
                AddToSeries("value", endTime, value);

                foreach (var kvp in storage.Where(x => x.Key != "value"))
                {
                    AddToSeries(kvp.Key, endTime, kvp.Value);
                }
            }
            else
            {
                var tick = baseData as Tick;
                var tradeBar = baseData as TradeBar;
                var quoteBar = baseData as QuoteBar;
                Add(tick, tradeBar, quoteBar);
            }
        }

        /// <summary>
        /// Adds Lean data objects to the end of the lists
        /// </summary>
        /// <param name="tick"><see cref="Tick"/> object that contains tick information of the security</param>
        /// <param name="tradeBar"><see cref="TradeBar"/> object that contains trade bar information of the security</param>
        /// <param name="quoteBar"><see cref="QuoteBar"/> object that contains quote bar information of the security</param>
        public void Add(Tick tick, TradeBar tradeBar, QuoteBar quoteBar)
        {
            if (tradeBar != null)
            {
                var time = tradeBar.EndTime;
                GetSerieWithCache(Open).Add(time, tradeBar.Open);
                GetSerieWithCache(High).Add(time, tradeBar.High);
                GetSerieWithCache(Low).Add(time, tradeBar.Low);
                GetSerieWithCache(Close).Add(time, tradeBar.Close);
                GetSerieWithCache(Volume).Add(time, tradeBar.Volume);
            }
            if (quoteBar != null)
            {
                var time = quoteBar.EndTime;
                if (tradeBar == null)
                {
                    GetSerieWithCache(Open).Add(time, quoteBar.Open);
                    GetSerieWithCache(High).Add(time, quoteBar.High);
                    GetSerieWithCache(Low).Add(time, quoteBar.Low);
                    GetSerieWithCache(Close).Add(time, quoteBar.Close);
                }
                if (quoteBar.Ask != null)
                {
                    GetSerieWithCache(AskOpen).Add(time, quoteBar.Ask.Open);
                    GetSerieWithCache(AskHigh).Add(time, quoteBar.Ask.High);
                    GetSerieWithCache(AskLow).Add(time, quoteBar.Ask.Low);
                    GetSerieWithCache(AskClose).Add(time, quoteBar.Ask.Close);
                    GetSerieWithCache(AskSize).Add(time, quoteBar.LastAskSize);
                }
                if (quoteBar.Bid != null)
                {
                    GetSerieWithCache(BidOpen).Add(time, quoteBar.Bid.Open);
                    GetSerieWithCache(BidHigh).Add(time, quoteBar.Bid.High);
                    GetSerieWithCache(BidLow).Add(time, quoteBar.Bid.Low);
                    GetSerieWithCache(BidClose).Add(time, quoteBar.Bid.Close);
                    GetSerieWithCache(BidSize).Add(time, quoteBar.LastBidSize);
                }
            }
            if (tick != null)
            {
                var time = tick.EndTime;

                // We will fill some series with null for tick types that don't have a value for that series, so that we make sure
                // the indices are the same for every tick series.

                if (tick.TickType == TickType.Quote)
                {
                    GetSerieWithCache(AskPrice).Add(time, tick.AskPrice);
                    GetSerieWithCache(AskSize).Add(time, tick.AskSize);
                    GetSerieWithCache(BidPrice).Add(time, tick.BidPrice);
                    GetSerieWithCache(BidSize).Add(time, tick.BidSize);
                }
                else
                {
                    // Trade and open interest ticks don't have these values, so we'll fill them with null.
                    GetSerieWithCache(AskPrice).Add(time, null);
                    GetSerieWithCache(AskSize).Add(time, null);
                    GetSerieWithCache(BidPrice).Add(time, null);
                    GetSerieWithCache(BidSize).Add(time, null);
                }

                GetSerieWithCache(Exchange).Add(time, tick.Exchange);
                GetSerieWithCache(Suspicious).Add(time, tick.Suspicious);
                GetSerieWithCache(Quantity).Add(time, tick.Quantity);

                if (tick.TickType == TickType.OpenInterest)
                {
                    GetSerieWithCache(OpenInterest).Add(time, tick.Value);
                    GetSerieWithCache(LastPrice).Add(time, null);
                }
                else
                {
                    GetSerieWithCache(LastPrice).Add(time, tick.Value);
                    GetSerieWithCache(OpenInterest).Add(time, null);
                }
            }
        }

        /// <summary>
        /// Get the pandas.DataFrame of the current <see cref="PandasData"/> state
        /// </summary>
        /// <param name="levels">Number of levels of the multi index</param>
        /// <returns>pandas.DataFrame object</returns>
        public PyObject ToPandasDataFrame(int levels = 2)
        {
            List<PyObject> list;
            var symbol = _symbol.ID.ToString().ToPython();

            // Create the index labels
            var names = _defaultNames;
            if (levels == 2)
            {
                // symbol, time
                names = _level2Names;
                list = new List<PyObject> { symbol, _empty };
            }
            else if (levels == 3)
            {
                // expiry, symbol, time
                names = _level3Names;
                list = new List<PyObject> { _symbol.ID.Date.ToPython(), symbol, _empty };
            }
            else
            {
                list = new List<PyObject> { _empty, _empty, _empty, symbol, _empty };
                if (_symbol.SecurityType == SecurityType.Future)
                {
                    list[0] = _symbol.ID.Date.ToPython();
                }
                else if (_symbol.SecurityType.IsOption())
                {
                    list[0] = _symbol.ID.Date.ToPython();
                    list[1] = _symbol.ID.StrikePrice.ToPython();
                    list[2] = _symbol.ID.OptionRight.ToString().ToPython();
                }
            }

            // creating the pandas MultiIndex is expensive so we keep a cash
            var indexCache = new Dictionary<List<DateTime>, PyObject>(new ListComparer<DateTime>());
            using (Py.GIL())
            {
                // Returns a dictionary keyed by column name where values are pandas.Series objects
                using var pyDict = new PyDict();
                foreach (var kvp in _series)
                {
                    if (kvp.Value.ShouldFilter) continue;
                    var values = kvp.Value.Values;

                    if (!indexCache.TryGetValue(kvp.Value.Times, out var index))
                    {
                        using var tuples = kvp.Value.Times.Select(time => CreateTupleIndex(time, list)).ToPyList();
                        using var namesDic = Py.kw("names", names);

                        indexCache[kvp.Value.Times] = index = _multiIndexFactory.Invoke(new[] { tuples }, namesDic);

                        foreach (var pyObject in tuples)
                        {
                            pyObject.Dispose();
                        }
                    }

                    // Adds pandas.Series value keyed by the column name
                    using var pyvalues = values.ToPyList();
                    using var series = _seriesFactory.Invoke(pyvalues, index);
                    pyDict.SetItem(kvp.Key, series);

                    foreach (var value in pyvalues)
                    {
                        value.Dispose();
                    }
                }
                _series.Clear();
                foreach (var kvp in indexCache)
                {
                    kvp.Value.Dispose();
                }

                for (var i = 0; i < list.Count; i++)
                {
                    DisposeIfNotEmpty(list[i]);
                }

                // Create the DataFrame
                var result = _dataFrameFactory.Invoke(pyDict);

                foreach (var item in pyDict)
                {
                    item.Dispose();
                }

                return result;
            }
        }

        /// <summary>
        /// Only dipose of the PyObject if it was set to something different than empty
        /// </summary>
        private static void DisposeIfNotEmpty(PyObject pyObject)
        {
            if (!ReferenceEquals(pyObject, _empty))
            {
                pyObject.Dispose();
            }
        }

        /// <summary>
        /// Create a new tuple index
        /// </summary>
        private static PyTuple CreateTupleIndex(DateTime index, List<PyObject> list)
        {
            DisposeIfNotEmpty(list[list.Count - 1]);
            list[list.Count - 1] = index.ToPython();
            return new PyTuple(list.ToArray());
        }

        /// <summary>
        /// Adds data to dictionary
        /// </summary>
        /// <param name="key">The key of the value to get</param>
        /// <param name="time"><see cref="DateTime"/> object to add to the value associated with the specific key</param>
        /// <param name="input"><see cref="Object"/> to add to the value associated with the specific key. Can be null.</param>
        private void AddToSeries(string key, DateTime time, object input)
        {
            var serie = GetSerieWithCache(key);
            serie.Add(time, input);
        }

        private Serie GetSerieWithCache(string key)
        {
            Serie serie;
            switch (key)
            {
                case Open:
                    _open ??= GetSerie(key);
                    serie = _open;
                    break;
                case High:
                    _high ??= GetSerie(key);
                    serie = _high;
                    break;
                case Low:
                    _low ??= GetSerie(key);
                    serie = _low;
                    break;
                case Close:
                    _close ??= GetSerie(key);
                    serie = _close;
                    break;
                case Volume:
                    _volume ??= GetSerie(key);
                    serie = _volume;
                    break;

                case AskOpen:
                    _askopen ??= GetSerie(key);
                    serie = _askopen;
                    break;
                case AskHigh:
                    _askhigh ??= GetSerie(key);
                    serie = _askhigh;
                    break;
                case AskLow:
                    _asklow ??= GetSerie(key);
                    serie = _asklow;
                    break;
                case AskClose:
                    _askclose ??= GetSerie(key);
                    serie = _askclose;
                    break;
                case AskSize:
                    _asksize ??= GetSerie(key);
                    serie = _asksize;
                    break;
                case AskPrice:
                    _askprice ??= GetSerie(key);
                    serie = _askprice;
                    break;

                case BidOpen:
                    _bidopen ??= GetSerie(key);
                    serie = _bidopen;
                    break;
                case BidHigh:
                    _bidhigh ??= GetSerie(key);
                    serie = _bidhigh;
                    break;
                case BidLow:
                    _bidlow ??= GetSerie(key);
                    serie = _bidlow;
                    break;
                case BidClose:
                    _bidclose ??= GetSerie(key);
                    serie = _bidclose;
                    break;
                case BidSize:
                    _bidsize ??= GetSerie(key);
                    serie = _bidsize;
                    break;
                case BidPrice:
                    _bidprice ??= GetSerie(key);
                    serie = _bidprice;
                    break;

                case LastPrice:
                    _lastPrice ??= GetSerie(key);
                    serie = _lastPrice;
                    break;
                case Exchange:
                    _exchange ??= GetSerie(key);
                    serie = _exchange;
                    break;
                case Quantity:
                    _quantity ??= GetSerie(key);
                    serie = _quantity;
                    break;
                case OpenInterest:
                    _openInterest ??= GetSerie(key);
                    serie = _openInterest;
                    break;
                case Suspicious:
                    _suspicious ??= GetSerie(key);
                    serie = _suspicious;
                    break;
                default:
                    serie = GetSerie(key);
                    break;
            }
            return serie;
        }

        private Serie GetSerie(string key)
        {
            if (!_series.TryGetValue(key, out var value))
            {
                throw new ArgumentException($"PandasData.GetSerie(): {Messages.PandasData.KeyNotFoundInSeries(key)}");
            }
            return value;
        }

        /// <summary>
        /// Get the lower-invariant name of properties of the type that a another type is assignable from
        /// </summary>
        /// <param name="baseType">The type that is assignable from</param>
        /// <param name="type">The type that is assignable by</param>
        /// <returns>List of string. Empty list if not assignable from</returns>
        private static IEnumerable<string> GetPropertiesNames(Type baseType, Type type)
        {
            return baseType.IsAssignableFrom(type)
                ? baseType.GetProperties().Select(x => x.Name.ToLowerInvariant())
                : Enumerable.Empty<string>();
        }

        private class Serie
        {
            public bool ShouldFilter { get; set; } = true;
            public List<DateTime> Times { get; set; } = new();
            public List<object> Values { get; set; } = new();

            public void Add(DateTime time, object input)
            {
                var value = input is decimal ? input.ConvertInvariant<double>() : input;
                if (ShouldFilter)
                {
                    // we need at least 1 valid entry for the series not to get filtered
                    if (value is double)
                    {
                        if (!((double)value).IsNaNOrZero())
                        {
                            ShouldFilter = false;
                        }
                    }
                    else if (value is string)
                    {
                        if (!string.IsNullOrWhiteSpace((string)value))
                        {
                            ShouldFilter = false;
                        }
                    }
                    else if (value is bool)
                    {
                        if ((bool)value)
                        {
                            ShouldFilter = false;
                        }
                    }
                    else if (value != null)
                    {
                        ShouldFilter = false;
                    }
                }

                Times.Add(time);
                Values.Add(value);
            }

            public void Add(DateTime time, decimal input)
            {
                var value = input.ConvertInvariant<double>();
                if (ShouldFilter && !value.IsNaNOrZero())
                {
                    ShouldFilter = false;
                }

                Times.Add(time);
                Values.Add(value);
            }
        }
    }
}
